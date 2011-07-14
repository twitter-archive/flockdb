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
import scala.collection.JavaConversions._
import com.twitter.gizzard.{Future, GizzardServer}
import com.twitter.gizzard.scheduler._
import com.twitter.gizzard.nameserver
import com.twitter.gizzard.shards.{ShardException, ShardInfo, ReplicatingShard, ShardId}
import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.gizzard.proxy.{ExceptionHandlingProxyFactory}
import com.twitter.logging.Logger
import com.twitter.gizzard.Stats
import com.twitter.querulous.StatsCollector
import com.twitter.querulous.database.DatabaseFactory
import com.twitter.querulous.evaluator.QueryEvaluatorFactory
import com.twitter.querulous.query.{QueryClass, QueryFactory}
import com.twitter.flockdb.conversions.Edge._
import com.twitter.flockdb.conversions.Metadata._
import com.twitter.flockdb.conversions.EdgeQuery._
import com.twitter.flockdb.conversions.EdgeResults._
import com.twitter.flockdb.conversions.ExecuteOperations._
import com.twitter.flockdb.conversions.Page._
import com.twitter.flockdb.conversions.Results._
import com.twitter.flockdb.conversions.SelectQuery._
import com.twitter.flockdb.conversions.SelectOperation._
import com.twitter.util.Duration
import queries._
import jobs.multi.{RemoveAll, Archive, Unarchive}
import jobs.single.{Add, Remove}
import Direction._
import thrift.FlockException
import config.{FlockDB => FlockDBConfig}

class FlockDB(config: FlockDBConfig) extends GizzardServer[shards.Shard](config) {
  object FlockExceptionWrappingProxyFactory extends ExceptionHandlingProxyFactory[thrift.FlockDB.Iface]({ (flock, e) =>
    e match {
      case _: thrift.FlockException =>
        throw e
      case _ =>
        exceptionLog.error(e, "Error in FlockDB.")
        throw new thrift.FlockException(e.toString)
    }
  })

  val stats = new StatsCollector {
    def incr(name: String, count: Int) = Stats.incr(name, count)
    def time[A](name: String)(f: => A): A = {
      val (rv, duration) = Duration.inMilliseconds(f)
      Stats.addMetric(name, duration.inMillis.toInt)
      rv
    }
  }

  val readWriteShardAdapter = new shards.ReadWriteShardAdapter(_)
  val jobPriorities = List(Priority.Low, Priority.Medium, Priority.High).map(_.id)
  val copyPriority = Priority.Medium.id
  val copyFactory = new jobs.CopyFactory(nameServer, jobScheduler(Priority.Medium.id))

  override val repairFactory = new jobs.RepairFactory(nameServer, jobScheduler)
  override val diffFactory = new jobs.DiffFactory(nameServer, jobScheduler)

  val dbQueryEvaluatorFactory = config.edgesQueryEvaluator(
    stats, { f => new TransactionStatsCollectingDatabaseFactory(f) }, { f => new TransactionStatsCollectingQueryFactory(f) })
  val lowLatencyQueryEvaluatorFactory = config.lowLatencyQueryEvaluator(
    stats, { f => new TransactionStatsCollectingDatabaseFactory(f) }, { f => new TransactionStatsCollectingQueryFactory(f) })
  val materializingQueryEvaluatorFactory = config.materializingQueryEvaluator(stats)

  shardRepo += ("com.twitter.flockdb.SqlShard" -> new shards.SqlShardFactory(dbQueryEvaluatorFactory, lowLatencyQueryEvaluatorFactory, materializingQueryEvaluatorFactory, config.databaseConnection))
  // for backward compat:
  shardRepo.setupPackage("com.twitter.service.flock.edges")
  shardRepo += ("com.twitter.service.flock.edges.SqlShard" -> new shards.SqlShardFactory(dbQueryEvaluatorFactory, lowLatencyQueryEvaluatorFactory, materializingQueryEvaluatorFactory, config.databaseConnection))

  val forwardingManager = new ForwardingManager(nameServer)

  jobCodec += ("single.Add".r, new jobs.single.AddParser(forwardingManager, OrderedUuidGenerator))
  jobCodec += ("single.Remove".r, new jobs.single.RemoveParser(forwardingManager, OrderedUuidGenerator))
  jobCodec += ("single.Archive".r, new jobs.single.ArchiveParser(forwardingManager, OrderedUuidGenerator))
  jobCodec += ("single.Negate".r, new jobs.single.NegateParser(forwardingManager, OrderedUuidGenerator))
  jobCodec += ("multi.Archive".r, new jobs.multi.ArchiveParser(forwardingManager, jobScheduler, config.aggregateJobsPageSize))
  jobCodec += ("multi.Unarchive".r, new jobs.multi.UnarchiveParser(forwardingManager, jobScheduler, config.aggregateJobsPageSize))
  jobCodec += ("multi.RemoveAll".r, new jobs.multi.RemoveAllParser(forwardingManager, jobScheduler, config.aggregateJobsPageSize))
  jobCodec += ("multi.Negate".r, new jobs.multi.NegateParser(forwardingManager, jobScheduler, config.aggregateJobsPageSize))

  jobCodec += ("jobs\\.(Copy|Migrate)".r, new jobs.CopyParser(nameServer, jobScheduler(Priority.Medium.id)))
  jobCodec += ("jobs\\.(MetadataCopy|MetadataMigrate)".r, new jobs.MetadataCopyParser(nameServer, jobScheduler(Priority.Medium.id)))

  jobCodec += ("jobs.Repair".r, new jobs.RepairParser(nameServer, jobScheduler))
  jobCodec += ("jobs.MetadataRepair".r, new jobs.MetadataRepairParser(nameServer, jobScheduler))

  jobCodec += ("jobs.Diff".r, new jobs.DiffParser(nameServer, jobScheduler))
  jobCodec += ("jobs.MetadataDiff".r, new jobs.MetadataDiffParser(nameServer, jobScheduler))

  val flockService = {
    val future = config.readFuture("readFuture")
    val edges = new EdgesService(nameServer, forwardingManager, copyFactory, jobScheduler, future, config.intersectionQuery, config.aggregateJobsPageSize)
    val scheduler = jobScheduler
    new FlockDBThriftAdapter(edges, scheduler)
  }

  private val loggingProxy = makeLoggingProxy[thrift.FlockDB.Iface]()
  lazy val loggingFlockService = loggingProxy(flockService)

  lazy val flockThriftServer = {
    val processor = new thrift.FlockDB.Processor(
      FlockExceptionWrappingProxyFactory(
        loggingFlockService))

    config.server(processor)
  }

  def start() {
    startGizzard()
    val runnable = new Runnable { def run() { flockThriftServer.serve() } }
    new Thread(runnable, "FlockDBServerThread").start()
  }

  def shutdown(quiesce: Boolean) {
    flockThriftServer.stop()
    shutdownGizzard(quiesce)
  }
}

class FlockDBThriftAdapter(val edges: EdgesService, val scheduler: PrioritizingJobScheduler) extends thrift.FlockDB.Iface {
  def contains(source_id: Long, graph_id: Int, destination_id: Long) = {
    edges.contains(source_id, graph_id, destination_id)
  }

  def get(source_id: Long, graph_id: Int, destination_id: Long) = {
    edges.get(source_id, graph_id, destination_id).toThrift
  }

  def get_metadata(source_id: Long, graph_id: Int) = {
    edges.getMetadata(source_id, graph_id).toThrift
  }

  def contains_metadata(source_id: Long, graph_id: Int) = {
    edges.containsMetadata(source_id, graph_id)
  }

  @deprecated("Use `select2` instead")
  def select(operations: JList[thrift.SelectOperation], page: thrift.Page): thrift.Results = {
    edges.select(new SelectQuery(operations.toSeq.map { _.fromThrift }, page.fromThrift)).toThrift
  }

  def select2(queries: JList[thrift.SelectQuery]): JList[thrift.Results] = {
    edges.select(queries.toSeq.map { _.fromThrift }).map { _.toThrift }
  }

  def select_edges(queries: JList[thrift.EdgeQuery]) = {
    edges.selectEdges(queries.toSeq.map { _.fromThrift }).map { _.toEdgeResults }
  }

  def execute(operations: thrift.ExecuteOperations) = {
    try {
      edges.execute(operations.fromThrift)
    } catch {
      case e: ShardException =>
        throw new FlockException(e.toString)
    }
  }

  @deprecated("Use `count2` instead")
  def count(query: JList[thrift.SelectOperation]) = {
    edges.count(List(query.toSeq.map { _.fromThrift })).first
  }

  def count2(queries: JList[JList[thrift.SelectOperation]]) = {
    edges.count(queries.toSeq.map { _.toSeq.map { _.fromThrift }}).pack
  }
}
