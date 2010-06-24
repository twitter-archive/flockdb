/*
 * Copyright 2010 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.twitter.flockdb

import java.lang.{Long => JLong, String}
import java.util.{ArrayList => JArrayList, List => JList}
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
import com.twitter.querulous.evaluator.QueryEvaluatorFactory
import com.twitter.querulous.query.QueryFactory
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


object FlockDB {
  def statsCollector(w3c: W3CStats) = {
    new StatsCollector {
      def incr(name: String, count: Int) = w3c.incr(name, count)
      def time[A](name: String)(f: => A): A = w3c.time(name)(f)
    }
  }

  def apply(config: ConfigMap, w3c: W3CStats): FlockDB = {
    val stats = statsCollector(w3c)
    val dbFactory = DatabaseFactory.fromConfig(config.configMap("db.connection_pool"), Some(stats))
    val nameServerDbFactory = DatabaseFactory.fromConfig(config.configMap("nameserver.connection_pool"), Some(stats))
    apply(config, dbFactory, nameServerDbFactory, w3c, stats)
  }

  def apply(config: ConfigMap, dbFactory: DatabaseFactory, nameServerDbFactory: DatabaseFactory, w3c: W3CStats, stats: StatsCollector): FlockDB = {
    val dbQueryFactory = QueryFactory.fromConfig(config.configMap("db"), Some(stats))
    val nameServerDbQueryFactory = QueryFactory.fromConfig(config.configMap("nameserver"), Some(stats))
    val dbQueryEvaluatorFactory = QueryEvaluatorFactory.fromConfig(config.configMap("db"), dbFactory, dbQueryFactory)
    val nameServerQueryEvaluatorFactory = QueryEvaluatorFactory.fromConfig(config.configMap("nameserver"), nameServerDbFactory, nameServerDbQueryFactory)

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
      val scheduler = JobScheduler(configName, queueConfig, jobParser)
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
      nameserver.ByteSwapper)
    val forwardingManager = new ForwardingManager(nameServer)
    val copyFactory = jobs.CopyFactory
    nameServer.reload()

    val singleJobParser = new jobs.single.JobParser(forwardingManager, OrderedUuidGenerator)
    val multiJobParser  = new jobs.multi.JobParser(forwardingManager, scheduler)
    val copyJobParser   = new BoundJobParser((nameServer, schedulerMap(Priority.Medium.id)))

    val future = new Future("EdgesFuture", config.configMap("edges.future"))

    polymorphicJobParser += ("\\.jobs\\.(Copy|Migrate|MetadataCopy|MetadataMigrate)".r, copyJobParser)
    polymorphicJobParser += ("flockdb\\.jobs\\.single".r, singleJobParser)
    polymorphicJobParser += ("flockdb\\.jobs\\.multi".r, multiJobParser)

    scheduler.start()

    new FlockDB(new EdgesService(nameServer, forwardingManager, copyFactory, scheduler, future))
  }
}

class FlockDB(val edges: EdgesService) extends thrift.FlockDB.Iface {
  def contains(source_id: Long, graph_id: Int, destination_id: Long) = {
    edges.contains(source_id, graph_id, destination_id)
  }

  def get(source_id: Long, graph_id: Int, destination_id: Long) = {
    edges.get(source_id, graph_id, destination_id).toThrift
  }

  @deprecated
  def select(operations: JList[thrift.SelectOperation], page: thrift.Page): thrift.Results = {
    edges.select(new SelectQuery(operations.toSeq.map { _.fromThrift }, page.fromThrift)).toThrift
  }

  def select2(queries: JList[thrift.SelectQuery]): JList[thrift.Results] = {
    edges.select(queries.toSeq.map { _.fromThrift }).map { _.toThrift }.toJavaList
  }

  def select_edges(queries: JList[thrift.EdgeQuery]) = {
    edges.selectEdges(queries.toSeq.map { _.fromThrift }).map { _.toEdgeResults }.toJavaList
  }

  def execute(operations: thrift.ExecuteOperations) = {
    edges.execute(operations.fromThrift)
  }

  @deprecated
  def count(query: JList[thrift.SelectOperation]) = {
    edges.count(List(query.toSeq.map { _.fromThrift })).first
  }

  def count2(queries: JList[JList[thrift.SelectOperation]]) = {
    edges.count(queries.toSeq.map { _.toSeq.map { _.fromThrift }}).pack
  }
}
