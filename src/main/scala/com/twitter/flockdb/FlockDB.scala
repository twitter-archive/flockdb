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
import com.twitter.gizzard.scheduler._
import com.twitter.gizzard.nameserver
import com.twitter.gizzard.shards.{ShardException, ShardInfo, ReplicatingShard}
import com.twitter.gizzard.thrift.conversions.Sequences._
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


object FlockDB {
  private val log = Logger.get(getClass.getName)

  def statsCollector(w3c: W3CStats) = {
    new StatsCollector {
      def incr(name: String, count: Int) = w3c.incr(name, count)
      def time[A](name: String)(f: => A): A = w3c.time(name)(f)
    }
  }

  def apply(config: ConfigMap, w3c: W3CStats): FlockDB = {
    @volatile val __trickJava = shards.FlockQueryClass.SelectModify

    val stats = statsCollector(w3c)
    val dbQueryEvaluatorFactory = QueryEvaluatorFactory.fromConfig(config.configMap("db"), Some(stats))
    val materializingQueryEvaluatorFactory = QueryEvaluatorFactory.fromConfig(config.configMap("materializing_db"), Some(stats))

    val codec = new JsonCodec[JsonJob]({ unparsable: Array[Byte] =>
      log.error("Unparsable job: %s", unparsable.map { n => "%02x".format(n.toInt & 0xff) }.mkString(", "))
    })

    val badJobQueue = new JsonJobLogger[JsonJob](Logger.get("bad_jobs"))
//  :(    val jobParser = new LoggingJobParser(Stats, w3c, new JobWithTasksParser(polymorphicJobParser))
    val scheduler = PrioritizingJobScheduler(config.configMap("edges.queue"), codec,
      Map(Priority.High.id -> "primary", Priority.Medium.id -> "copy", Priority.Low.id -> "slow"),
      Some(badJobQueue))


    val replicationFuture = new Future("ReplicationFuture", config.configMap("edges.replication.future"))
    val shardRepository = new nameserver.BasicShardRepository[shards.Shard](
      new shards.ReadWriteShardAdapter(_), Some(replicationFuture))
    shardRepository += ("com.twitter.flockdb.SqlShard" -> new shards.SqlShardFactory(dbQueryEvaluatorFactory, materializingQueryEvaluatorFactory, config))
    // for backward compat:
    shardRepository.setupPackage("com.twitter.service.flock.edges")
    shardRepository += ("com.twitter.service.flock.edges.SqlShard" -> new shards.SqlShardFactory(dbQueryEvaluatorFactory, materializingQueryEvaluatorFactory, config))

    val nameServer = nameserver.NameServer(config.configMap("edges.nameservers"), Some(stats),
                                           shardRepository, Some(replicationFuture))

    val forwardingManager = new ForwardingManager(nameServer)
    nameServer.reload()

    codec += ("single.Add".r, new jobs.single.AddParser(forwardingManager, OrderedUuidGenerator))
    codec += ("single.Remove".r, new jobs.single.RemoveParser(forwardingManager, OrderedUuidGenerator))
    codec += ("single.Archive".r, new jobs.single.ArchiveParser(forwardingManager, OrderedUuidGenerator))
    codec += ("single.Negate".r, new jobs.single.NegateParser(forwardingManager, OrderedUuidGenerator))
    codec += ("multi.Archive".r, new jobs.multi.ArchiveParser(forwardingManager, scheduler))
    codec += ("multi.Unarchive".r, new jobs.multi.UnarchiveParser(forwardingManager, scheduler))
    codec += ("multi.RemoveAll".r, new jobs.multi.RemoveAllParser(forwardingManager, scheduler))
    codec += ("multi.Negate".r, new jobs.multi.NegateParser(forwardingManager, scheduler))

    codec += ("(Copy|Migrate)".r, new jobs.CopyParser(nameServer, scheduler(Priority.Medium.id)))
    codec += ("(MetadataCopy|MetadataMigrate)".r, new jobs.MetadataCopyParser(nameServer, scheduler(Priority.Medium.id)))

    val future = new Future("EdgesFuture", config.configMap("edges.future"))

    scheduler.start()

    val copyFactory = new jobs.CopyFactory(nameServer, scheduler(Priority.Medium.id))
    new FlockDB(new EdgesService(nameServer, forwardingManager, copyFactory, scheduler,
                                 future, replicationFuture))
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
