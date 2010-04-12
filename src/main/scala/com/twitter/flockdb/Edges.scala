package com.twitter.flockdb

import java.lang.{Long => JLong, String}
import java.util.{List => JList, ArrayList => JArrayList}
import scala.collection.mutable
import com.twitter.gizzard.Future
import com.twitter.gizzard.jobs._
import com.twitter.gizzard.scheduler.{KestrelMessageQueue, JobScheduler, PrioritizingJobScheduler}
import com.twitter.gizzard.nameserver
import com.twitter.gizzard.nameserver.{NameServer, ShardRepository}
import com.twitter.gizzard.shards.{ShardInfo, ReplicatingShard}
import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.service.flock.conversions.Page._
import com.twitter.service.flock.conversions.Results._
import com.twitter.results.{Cursor, ResultWindow}
import com.twitter.ostrich.Stats
import com.twitter.querulous.StatsCollector
import com.twitter.querulous.database.DatabaseFactory
import com.twitter.querulous.evaluator.{AutoDisablingQueryEvaluatorFactory, StandardQueryEvaluatorFactory}
import com.twitter.querulous.query.{TimingOutQueryFactory, SqlQueryFactory}
import com.twitter.service.flock
import com.twitter.service.flock.{ByteSwapper, OrderedUuidGenerator, TimingOutStatsCollectingQueryFactory, State}
import com.twitter.flockdb.conversions.Edge._
import com.twitter.flockdb.conversions.EdgeQuery._
import com.twitter.flockdb.conversions.EdgeResults._
import com.twitter.flockdb.conversions.ExecuteOperations._
import com.twitter.flockdb.conversions.SelectQuery._
import com.twitter.service.flock.thrift.FlockException
import com.twitter.xrayspecs.{Duration, Time}
import com.twitter.xrayspecs.TimeConversions._
import net.lag.configgy.{Config, ConfigMap}
import net.lag.logging.{Logger, ThrottledLogger}
import queries._
import jobs.multi.{RemoveAll, Archive, Unarchive}
import jobs.single.{Add, Remove}
import Direction._
import conversions.SelectOperation._
import com.twitter.ostrich.W3CStats


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

  def apply(config: ConfigMap, w3c: W3CStats, databaseFactory: DatabaseFactory) = {
    val sqlQueryFactory = new SqlQueryFactory
    val stats = new StatsCollector {
      def incr(name: String, count: Int) = w3c.incr(name, count)
      def time[A](name: String)(f: => A): A = w3c.time(name)(f)
    }
    val (dbQueryInfo, nameServerQueryInfo) = (convertConfigMap(config.configMap("db.queries")), convertConfigMap(config.configMap("nameserver.queries")))
    val dbQueryTimeoutDefault              = config("db.query_timeout_default").toLong.millis
    val dbQueryFactory                     = new TimingOutStatsCollectingQueryFactory(sqlQueryFactory, dbQueryInfo, dbQueryTimeoutDefault, stats)
    val dbQueryEvaluatorFactory = new AutoDisablingQueryEvaluatorFactory(new StandardQueryEvaluatorFactory(
      databaseFactory,
      dbQueryFactory),
      config("db.disable.error_count").toInt,
      config("db.disable.seconds").toInt.seconds)
    val nameServerQueryTimeoutDefault = config("nameserver.query_timeout_default").toLong.millis
    val nameServerQueryFactory = new TimingOutStatsCollectingQueryFactory(sqlQueryFactory, nameServerQueryInfo, nameServerQueryTimeoutDefault, stats)
    val nameServerQueryEvaluatorFactory = new AutoDisablingQueryEvaluatorFactory(new StandardQueryEvaluatorFactory(
      databaseFactory,
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

    val shardRepository = new ShardRepository[shards.Shard]
    val log = new ThrottledLogger[String](Logger(), config("throttled_log.period_msec").toInt, config("throttled_log.rate").toInt)
    val replicationFuture = new Future("ReplicationFuture", config.configMap("edges.replication.future"))
    shardRepository += ("com.twitter.flockdb.SqlShard" -> new shards.SqlShardFactory(dbQueryEvaluatorFactory, nameServerQueryEvaluatorFactory, config))
    shardRepository += ("com.twitter.flockdb.ReplicatingShard" -> new shards.ReplicatingShardFactory(log, replicationFuture))

    val queueConfig          = config.configMap("edges.queue")
    val polymorphicJobParser = new PolymorphicJobParser
    val jobParser            = new JobWithTasksParser(polymorphicJobParser)

    val primaryScheduler   = JobScheduler("primary", queueConfig, jobParser, w3c)
    val copyScheduler      = JobScheduler("copy", queueConfig, jobParser, w3c)
    val slowScheduler      = JobScheduler("slow", queueConfig, jobParser, w3c)

    val prioritizingJobScheduler =
      new PrioritizingJobScheduler(Map(Priority.Low.id -> slowScheduler,
                                       Priority.Medium.id -> copyScheduler,
                                       Priority.High.id -> primaryScheduler))

    val replicatingNameServerShard = new nameserver.ReadWriteShardAdapter(new ReplicatingShard(new ShardInfo("com.twitter.nameserver.ReplicatingShard", "", ""), 0, nameServerShards, new nameserver.LoadBalancer(nameServerShards), log, replicationFuture))
    val nameServer = new NameServer(replicatingNameServerShard, shardRepository, ByteSwapper)
    val forwardingManager = new ForwardingManager(nameServer)
    val copyFactory = jobs.CopyFactory
    nameServer.reload()

    val singleJobParser = new jobs.single.JobParser(forwardingManager, OrderedUuidGenerator)
    val multiJobParser  = new jobs.multi.JobParser(forwardingManager, prioritizingJobScheduler)
    val copyJobParser   = new BoundJobParser((nameServer, copyScheduler))

    val future = new Future("EdgesFuture", config.configMap("edges.future"))

    polymorphicJobParser += ("flockdb\\.jobs\\.(Copy|Migrate|MetadataCopy|MetadataMigrate)".r, copyJobParser)
    polymorphicJobParser += ("flockdb\\.jobs\\.single".r, singleJobParser)
    polymorphicJobParser += ("flockdb\\.jobs\\.multi".r, multiJobParser)

    prioritizingJobScheduler.start()

    new Edges(nameServer, forwardingManager, copyFactory, prioritizingJobScheduler, future)
  }
}

class Edges(val nameServer: NameServer[shards.Shard], val forwardingManager: ForwardingManager, val copyFactory: gizzard.jobs.CopyFactory[shards.Shard],
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
  def select(operations: JList[thrift.SelectOperation], page: flock.thrift.Page): flock.thrift.Results = {
    selectCompiler(operations.toSeq.map { _.fromThrift }).select(page.fromThrift).toThrift
  }

  def select2(queries: JList[thrift.SelectQuery]): JList[flock.thrift.Results] = {
    queries.toSeq.parallel(future).map[flock.thrift.Results] { query =>
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
