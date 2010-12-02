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
import com.twitter.gizzard.{Future, GizzardServer}
import com.twitter.gizzard.scheduler._
import com.twitter.gizzard.nameserver
import com.twitter.gizzard.shards.{ShardException, ShardInfo, ReplicatingShard}
import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.gizzard.proxy.{ExceptionHandlingProxy, LoggingProxy}
import com.twitter.ostrich.{Stats, W3CStats}
import com.twitter.querulous.StatsCollector
import com.twitter.querulous.database.DatabaseFactory
import com.twitter.querulous.evaluator.QueryEvaluatorFactory
import com.twitter.querulous.query.{QueryClass, QueryFactory}
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
import net.lag.logging.Logger
import queries._
import jobs.multi.{RemoveAll, Archive, Unarchive}
import jobs.single.{Add, Remove}
import Direction._
import thrift.FlockException

class FlockDB(config: flockdb.config.FlockDB, w3c: W3CStats) extends GizzardServer[shards.Shard, JsonJob](config) {

  object FlockExceptionWrappingProxy extends ExceptionHandlingProxy({ e =>
    e match {
      case _: thrift.FlockException =>
        throw e
      case _ =>
        log.error(e, "Error in FlockDB: " + e)
        throw new thrift.FlockException(e.toString)
    }
  })

  val stats = new StatsCollector {
    def incr(name: String, count: Int) = w3c.incr(name, count)
    def time[A](name: String)(f: => A): A = w3c.time(name)(f)
  }

  @volatile val __trickJava = List(
    shards.FlockQueryClass.SelectModify,
    shards.FlockQueryClass.SelectCopy)

  val readWriteShardAdapter = new shards.ReadWriteShardAdapter(_)
  val jobPriorities = List(Priority.Low, Priority.Medium, Priority.High).map(_.id)
  val copyPriority = Priority.Medium.id
  val copyFactory = new jobs.CopyFactory(nameServer, jobScheduler(Priority.Medium.id))

  val dbQueryEvaluatorFactory = config.edgesQueryEvaluator(stats)
  val materializingQueryEvaluatorFactory = config.materializingQueryEvaluator(stats)

  shardRepo += ("com.twitter.flockdb.SqlShard" -> new shards.SqlShardFactory(dbQueryEvaluatorFactory, materializingQueryEvaluatorFactory, config.databaseConnection))
  // for backward compat:
  shardRepo.setupPackage("com.twitter.service.flock.edges")
  shardRepo += ("com.twitter.service.flock.edges.SqlShard" -> new shards.SqlShardFactory(dbQueryEvaluatorFactory, materializingQueryEvaluatorFactory, config.databaseConnection))

  val forwardingManager = new ForwardingManager(nameServer)

  jobCodec += ("single.Add".r, new jobs.single.AddParser(forwardingManager, OrderedUuidGenerator))
  jobCodec += ("single.Remove".r, new jobs.single.RemoveParser(forwardingManager, OrderedUuidGenerator))
  jobCodec += ("single.Archive".r, new jobs.single.ArchiveParser(forwardingManager, OrderedUuidGenerator))
  jobCodec += ("single.Negate".r, new jobs.single.NegateParser(forwardingManager, OrderedUuidGenerator))
  jobCodec += ("multi.Archive".r, new jobs.multi.ArchiveParser(forwardingManager, jobScheduler))
  jobCodec += ("multi.Unarchive".r, new jobs.multi.UnarchiveParser(forwardingManager, jobScheduler))
  jobCodec += ("multi.RemoveAll".r, new jobs.multi.RemoveAllParser(forwardingManager, jobScheduler))
  jobCodec += ("multi.Negate".r, new jobs.multi.NegateParser(forwardingManager, jobScheduler))

  jobCodec += ("jobs\\.(Copy|Migrate)".r, new jobs.CopyParser(nameServer, jobScheduler(Priority.Medium.id)))
  jobCodec += ("jobs\\.(MetadataCopy|MetadataMigrate)".r, new jobs.MetadataCopyParser(nameServer, jobScheduler(Priority.Medium.id)))

  val flockService = {
    val future = config.readFuture("readFuture")
    val edges = new EdgesService(nameServer, forwardingManager, copyFactory, jobScheduler, future)
    new FlockDBThriftAdapter(edges)
  }

  lazy val flockThriftServer = {
    val processor = new flockdb.thrift.FlockDB.Processor(
      FlockExceptionWrappingProxy(
        LoggingProxy[flockdb.thrift.FlockDB.Iface](
          Stats, w3c, "FlockDB",
          flockService)))

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

class FlockDBThriftAdapter(val edges: EdgesService) extends thrift.FlockDB.Iface {
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
    try {
      edges.execute(operations.fromThrift)
    } catch {
      case e: ShardException =>
        throw new FlockException(e.toString)
    }
  }

  @deprecated
  def count(query: JList[thrift.SelectOperation]) = {
    edges.count(List(query.toSeq.map { _.fromThrift })).first
  }

  def count2(queries: JList[JList[thrift.SelectOperation]]) = {
    edges.count(queries.toSeq.map { _.toSeq.map { _.fromThrift }}).pack
  }
}
