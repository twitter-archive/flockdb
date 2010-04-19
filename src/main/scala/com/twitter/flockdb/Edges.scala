package com.twitter.flockdb

import java.lang.{Long => JLong, String}
import java.util.{ArrayList => JArrayList, List => JList, Random}
import scala.collection.mutable
import com.twitter.gizzard.Future
import com.twitter.gizzard.jobs._
import com.twitter.gizzard.scheduler.{KestrelMessageQueue, JobScheduler, PrioritizingJobScheduler}
import com.twitter.gizzard.nameserver
import com.twitter.gizzard.shards.{ShardInfo, ReplicatingShard}
import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.results.{Cursor, ResultWindow}
import com.twitter.ostrich.{Stats, W3CStats}
import com.twitter.querulous.StatsCollector
import com.twitter.querulous.database.DatabaseFactory
import com.twitter.querulous.evaluator.{AutoDisablingQueryEvaluatorFactory, StandardQueryEvaluatorFactory}
import com.twitter.querulous.query.{TimingOutQueryFactory, TimingOutStatsCollectingQueryFactory, SqlQueryFactory}
import com.twitter.flockdb.conversions.Edge._
import com.twitter.flockdb.conversions.EdgeQuery._
import com.twitter.flockdb.conversions.EdgeResults._
import com.twitter.flockdb.conversions.ExecuteOperations._
import com.twitter.flockdb.conversions.Page._
import com.twitter.flockdb.conversions.Results._
import com.twitter.flockdb.conversions.SelectQuery._
import com.twitter.flockdb.conversions.SelectOperation._
import com.twitter.xrayspecs.{Duration, Time}
import com.twitter.xrayspecs.TimeConversions._
import net.lag.configgy.{Config, ConfigMap}
import net.lag.logging.{Logger, ThrottledLogger}
import queries._
import jobs.multi.{RemoveAll, Archive, Unarchive}
import jobs.single.{Add, Remove}
import Direction._
import thrift.FlockException


object Edges {
  def convertConfigMap(queryMap: ConfigMap) = {
    val queryInfo = new mutable.HashMap[String, (String, Duration)]
    for (key <- queryMap.keys) {
      val pair = queryMap.getList(key)
      val query = pair(0)
      val timeout = pair(1).toLong.millis
      queryInfo += (query -> (key, timeout))
    }
    queryInfo
  }

  val random = new Random()
  def generateShardId() = {
    (random.nextInt() & ((1 << 30) - 1)).toInt
  }

  def statsCollector(w3c: W3CStats) = {
    new StatsCollector {
      def incr(name: String, count: Int) = w3c.incr(name, count)
      def time[A](name: String)(f: => A): A = w3c.time(name)(f)
    }
  }

  def apply(config: ConfigMap, w3c: W3CStats): Edges = {
    val stats = statsCollector(w3c)
    val dbFactory = AutoDatabaseFactory(config.configMap("db.connection_pool"), Some(stats))
    val nameServerDbFactory = AutoDatabaseFactory(config.configMap("nameserver.connection_pool"), Some(stats))
    apply(config, dbFactory, nameServerDbFactory, w3c, stats)
  }

  def apply(config: ConfigMap, dbFactory: DatabaseFactory, nameServerDbFactory: DatabaseFactory, w3c: W3CStats, stats: StatsCollector): Edges = {
    val sqlQueryFactory = new SqlQueryFactory
    val (dbQueryInfo, nameServerQueryInfo) = (convertConfigMap(config.configMap("db.queries")), convertConfigMap(config.configMap("nameserver.queries")))

    val dbQueryTimeoutDefault = config("db.query_timeout_default").toLong.millis
    val dbQueryFactory = new TimingOutStatsCollectingQueryFactory(sqlQueryFactory, dbQueryInfo, dbQueryTimeoutDefault, stats)
    val dbQueryEvaluatorFactory = new AutoDisablingQueryEvaluatorFactory(new StandardQueryEvaluatorFactory(
      dbFactory,
      dbQueryFactory),
      config("db.disable.error_count").toInt,
      config("db.disable.seconds").toInt.seconds)

    val nameServerQueryTimeoutDefault = config("nameserver.query_timeout_default").toLong.millis
    val nameServerQueryFactory = new TimingOutStatsCollectingQueryFactory(sqlQueryFactory, nameServerQueryInfo, nameServerQueryTimeoutDefault, stats)
    val nameServerQueryEvaluatorFactory = new AutoDisablingQueryEvaluatorFactory(new StandardQueryEvaluatorFactory(
      nameServerDbFactory,
      nameServerQueryFactory),
      config("nameserver.disable.error_count").toInt,
      config("nameserver.disable.seconds").toInt.seconds)

    val nameServerConfig = config.configMap("edges.nameservers")
    val nameServerShards = (for (key <- nameServerConfig.keys) yield {
      val map = nameServerConfig.configMap(key)
      nameServerQueryEvaluatorFactory(map("hostname"), map("database"), map("username"), map("password"))
    }) map { queryEvaluator =>
      new nameserver.SqlShard(queryEvaluator)
    } collect

    val log = new ThrottledLogger[String](Logger(), config("throttled_log.period_msec").toInt, config("throttled_log.rate").toInt)
    val replicationFuture = new Future("ReplicationFuture", config.configMap("edges.replication.future"))
    val shardRepository = new nameserver.BasicShardRepository[shards.Shard](
      new shards.ReadWriteShardAdapter(_), log, replicationFuture)
    shardRepository += ("com.twitter.flockdb.SqlShard" -> new shards.SqlShardFactory(dbQueryEvaluatorFactory, nameServerQueryEvaluatorFactory, config))
    // for backward compat:
    shardRepository.setupPackage("com.twitter.service.flock.edges")
    shardRepository += ("com.twitter.service.flock.edges.SqlShard" -> new shards.SqlShardFactory(dbQueryEvaluatorFactory, nameServerQueryEvaluatorFactory, config))
    shardRepository += ("com.twitter.service.flock.edges.BlackHoleShard" -> new shards.BlackHoleShardFactory)

    val polymorphicJobParser = new PolymorphicJobParser
    val jobParser = new LoggingJobParser(Stats, w3c, new JobWithTasksParser(polymorphicJobParser))

    val schedulerMap = new mutable.HashMap[Int, JobScheduler]
    List((Priority.High, "primary"), (Priority.Medium, "copy"),
         (Priority.Low, "slow")).foreach { case (priority, configName) =>
      val queueConfig = config.configMap("edges.queue")
      val scheduler = JobScheduler(configName, queueConfig, jobParser, w3c)
      schedulerMap(priority.id) = scheduler
    }
    val scheduler = new PrioritizingJobScheduler(schedulerMap)

    val replicatingNameServerShardInfo =
      new ShardInfo("com.twitter.gizzard.nameserver.ReplicatingShard", "", "")
    val replicatingNameServerShard = new nameserver.ReadWriteShardAdapter(
      new ReplicatingShard(replicatingNameServerShardInfo, 0, nameServerShards,
                           new nameserver.LoadBalancer(nameServerShards), log,
                           replicationFuture))

    val nameServer = new nameserver.NameServer(replicatingNameServerShard, shardRepository,
      nameserver.ByteSwapper, generateShardId)
    val forwardingManager = new ForwardingManager(nameServer)
    val copyFactory = jobs.CopyFactory
    nameServer.reload()

    val singleJobParser = new jobs.single.JobParser(forwardingManager, OrderedUuidGenerator)
    val multiJobParser  = new jobs.multi.JobParser(forwardingManager, scheduler)
    val copyJobParser   = new BoundJobParser((nameServer, schedulerMap(Priority.Medium.id)))

    val future = new Future("EdgesFuture", config.configMap("edges.future"))

    polymorphicJobParser += ("flockdb\\.jobs\\.(Copy|Migrate|MetadataCopy|MetadataMigrate)".r, copyJobParser)
    polymorphicJobParser += ("flockdb\\.jobs\\.single".r, singleJobParser)
    polymorphicJobParser += ("flockdb\\.jobs\\.multi".r, multiJobParser)

    scheduler.start()

    new Edges(nameServer, forwardingManager, copyFactory, scheduler, future)
  }
}

class Edges(val nameServer: nameserver.NameServer[shards.Shard],
            val forwardingManager: ForwardingManager,
            val copyFactory: gizzard.jobs.CopyFactory[shards.Shard],
            val schedule: PrioritizingJobScheduler, future: Future)
  extends thrift.Edges.Iface {

  private val selectCompiler = new SelectCompiler(forwardingManager)
  private val executeCompiler = new ExecuteCompiler(schedule)

  def counts_of_destinations_for(source_ids: Array[Byte], graph_id: Int) = {
    new shards.MultiShard(forwardingManager, source_ids.toLongArray, graph_id, Forward).counts.pack
  }

  def counts_of_sources_for(destination_ids: Array[Byte], graph_id: Int) = {
    new shards.MultiShard(forwardingManager, destination_ids.toLongArray, graph_id, Backward).counts.pack
  }

  def contains(source_id: Long, graph_id: Int, destination_id: Long) = {
    forwardingManager.find(source_id, graph_id, Forward).get(source_id, destination_id).map { edge =>
      edge.state == State.Normal || edge.state == State.Negative
    }.getOrElse(false)
  }

  def get(source_id: Long, graph_id: Int, destination_id: Long) = {
    forwardingManager.find(source_id, graph_id, Forward).get(source_id, destination_id).map {
      _.toThrift
    } getOrElse {
      throw new FlockException("Record not found: (%d, %d, %d)".format(source_id, graph_id, destination_id))
    }
  }

  @deprecated
  def select(operations: JList[thrift.SelectOperation], page: thrift.Page): thrift.Results = {
    selectCompiler(operations.toSeq.map { _.fromThrift }).select(page.fromThrift).toThrift
  }

  def select2(queries: JList[thrift.SelectQuery]): JList[thrift.Results] = {
    queries.toSeq.parallel(future).map[thrift.Results] { query =>
      selectCompiler(query.fromThrift.operations.toSeq).select(query.page.fromThrift).toThrift
    }.toJavaList
  }

  def select_edges(queries: JList[thrift.EdgeQuery]) = {
    queries.toSeq.parallel(future).map[thrift.EdgeResults] { query =>
      val term = query.term
      val shard = forwardingManager.find(term.source_id, term.graph_id, Direction(term.is_forward))
      val states = if (term.isSetState_ids) term.state_ids.toSeq.map(State(_)) else List(State.Normal)

      if (term.destination_ids == null) {
        shard.selectEdges(term.source_id, states, query.page.count, Cursor(query.page.cursor)).toEdgeResults
      } else {
        val results = shard.intersectEdges(term.source_id, states, term.destination_ids.toLongArray)
        new ResultWindow(results.map { edge => (edge, Cursor(edge.destinationId)) }, query.page.count, Cursor(query.page.cursor)).toEdgeResults
      }
    }.toJavaList
  }

  def execute(operations: thrift.ExecuteOperations) = {
    executeCompiler(operations.fromThrift)
  }

  @deprecated
  def count(query: JList[thrift.SelectOperation]) = {
    selectCompiler(query.toSeq.map { _.fromThrift }).sizeEstimate
  }

  def count2(queries: JList[JList[thrift.SelectOperation]]) = {
    queries.toSeq.parallel(future).map[Int] { query =>
      selectCompiler(query.toSeq.map { _.fromThrift }).sizeEstimate
    }.pack
  }
}
