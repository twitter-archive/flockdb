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

import com.twitter.util.Future
import com.twitter.util.Duration
import com.twitter.ostrich.admin.Service
import com.twitter.querulous.StatsCollector
import com.twitter.gizzard.GizzardServer
import com.twitter.gizzard.scheduler._
import com.twitter.gizzard.Stats
import com.twitter.flockdb.shards.{Shard, SqlShardFactory}
import com.twitter.flockdb.config.{FlockDB => FlockDBConfig}


class FlockDB(config: FlockDBConfig) extends GizzardServer(config) with Service {
  val stats = new StatsCollector {
    def incr(name: String, count: Int) = Stats.incr(name, count)
    def time[A](name: String)(f: => A): A = {
      val (rv, duration) = Duration.inMilliseconds(f)
      Stats.addMetric(name, duration.inMillis.toInt)
      rv
    }
    override def addGauge(name: String)(gauge: => Double) { Stats.addGauge(name)(gauge) }
  }

  val jobPriorities = List(Priority.Low, Priority.Medium, Priority.High).map(_.id)
  val copyPriority = Priority.Medium.id

  val shardFactory = new SqlShardFactory(
    config.edgesQueryEvaluator(
      stats,
      new TransactionStatsCollectingDatabaseFactory(_),
      new TransactionStatsCollectingQueryFactory(_)
    ),
    config.lowLatencyQueryEvaluator(
      stats,
      new TransactionStatsCollectingDatabaseFactory(_),
      new TransactionStatsCollectingQueryFactory(_)
    ),
    config.materializingQueryEvaluator(stats),
    config.databaseConnection
  )

  nameServer.configureMultiForwarder[Shard] {
    _.shardFactories(
      "com.twitter.flockdb.SqlShard" -> shardFactory,
      "com.twitter.service.flock.edges.SqlShard" -> shardFactory
    )
    .copyFactory(new jobs.CopyFactory(nameServer, jobScheduler(Priority.Medium.id)))
  }

  val forwardingManager = new ForwardingManager(nameServer.multiTableForwarder[Shard])

  jobCodec += ("single.Single".r, new jobs.single.SingleJobParser(forwardingManager, OrderedUuidGenerator))
  jobCodec += ("multi.Multi".r,   new jobs.multi.MultiJobParser(forwardingManager, jobScheduler, config.aggregateJobsPageSize))

  jobCodec += ("jobs\\.(Copy|Migrate)".r,                 new jobs.CopyParser(nameServer, jobScheduler(Priority.Medium.id)))
  jobCodec += ("jobs\\.(MetadataCopy|MetadataMigrate)".r, new jobs.MetadataCopyParser(nameServer, jobScheduler(Priority.Medium.id)))

  // XXX: remove when old tagged jobs no longer exist.
  import jobs.LegacySingleJobParser
  import jobs.LegacyMultiJobParser
  jobCodec += ("single.Add".r,      LegacySingleJobParser.Add(forwardingManager, OrderedUuidGenerator))
  jobCodec += ("single.Remove".r,   LegacySingleJobParser.Remove(forwardingManager, OrderedUuidGenerator))
  jobCodec += ("single.Archive".r,  LegacySingleJobParser.Archive(forwardingManager, OrderedUuidGenerator))
  jobCodec += ("single.Negate".r,   LegacySingleJobParser.Negate(forwardingManager, OrderedUuidGenerator))
  jobCodec += ("multi.Archive".r,   LegacyMultiJobParser.Archive(forwardingManager, jobScheduler, config.aggregateJobsPageSize))
  jobCodec += ("multi.Unarchive".r, LegacyMultiJobParser.Unarchive(forwardingManager, jobScheduler, config.aggregateJobsPageSize))
  jobCodec += ("multi.RemoveAll".r, LegacyMultiJobParser.RemoveAll(forwardingManager, jobScheduler, config.aggregateJobsPageSize))
  jobCodec += ("multi.Negate".r,    LegacyMultiJobParser.Negate(forwardingManager, jobScheduler, config.aggregateJobsPageSize))

  val flockService = new EdgesService(
    forwardingManager,
    jobScheduler,
    config.intersectionQuery,
    config.aggregateJobsPageSize
  )

  private val flockThriftServer = {
    val flockThriftIface = new FlockDBThriftAdapter(flockService)
    val loggingProxy = makeLoggingProxy[thrift.FlockDB.FutureIface]()
    val loggingFlockThriftIface = loggingProxy(flockThriftIface)
    new FlockDBThriftServer(
      config.server.name,
      config.server.port,
      config.server.maxConcurrentRequests,
      loggingFlockThriftIface
    )
  }

  // satisfy service

  def start() {
    startGizzard()
    flockThriftServer.start()
  }

  def shutdown() {
    flockThriftServer.shutdown()
    shutdownGizzard(false)
  }

  override def quiesce() {
    flockThriftServer.shutdown()
    shutdownGizzard(true)
  }
}

class FlockDBThriftServer(
  val serverName: String,
  val thriftPort: Int,
  val maxConcurrentRequests: Int,
  val ifaceImpl: thrift.FlockDB.FutureIface)
extends thrift.FlockDB.ThriftServer {
  override def serverBuilder = super.serverBuilder.maxConcurrentRequests(maxConcurrentRequests)

  def contains(`sourceId`: Long, `graphId`: Int, `destinationId`: Long) = {
    ifaceImpl.contains(sourceId, graphId, destinationId)
  }

  def get(`sourceId`: Long, `graphId`: Int, `destinationId`: Long) = {
    ifaceImpl.get(sourceId, graphId, destinationId)
  }

  def getMetadata(`sourceId`: Long, `graphId`: Int) = {
    ifaceImpl.getMetadata(sourceId, graphId)
  }

  def containsMetadata(`sourceId`: Long, `graphId`: Int) = {
    ifaceImpl.containsMetadata(sourceId, graphId)
  }

  def select2(`queries`: Seq[thrift.SelectQuery]) = {
    ifaceImpl.select2(queries)
  }

  def count2(`queries`: Seq[Seq[thrift.SelectOperation]]) = {
    ifaceImpl.count2(queries)
  }

  def selectEdges(`queries`: Seq[thrift.EdgeQuery]) = {
    ifaceImpl.selectEdges(queries)
  }

  def execute(`operations`: thrift.ExecuteOperations) = {
    ifaceImpl.execute(operations)
  }

  def count(`operations`: Seq[thrift.SelectOperation]) = {
    ifaceImpl.count(operations)
  }

  def select(`operations`: Seq[thrift.SelectOperation], `page`: thrift.Page) = {
    ifaceImpl.select(operations, page)
  }
}

class FlockDBThriftAdapter(val edges: EdgesService) extends thrift.FlockDB.FutureIface {
  import com.twitter.flockdb.operations._
  import java.nio.{BufferUnderflowException, ByteBuffer, ByteOrder}

  def contains(sourceId: Long, graphId: Int, destinationId: Long) = {
    edges.contains(sourceId, graphId, destinationId)
  }

  def get(sourceId: Long, graphId: Int, destinationId: Long) = {
    edges.get(sourceId, graphId, destinationId) map { edgeToThrift(_) }
  }

  def getMetadata(sourceId: Long, graphId: Int) = {
    edges.getMetadata(sourceId, graphId) map { metadataToThrift(_) }
  }

  def containsMetadata(sourceId: Long, graphId: Int) = {
    edges.containsMetadata(sourceId, graphId)
  }

  @deprecated("Use `select2` instead")
  def select(operations: Seq[thrift.SelectOperation], page: thrift.Page) = {
    select2(Seq(thrift.SelectQuery(operations, page))) map { _.head }
  }

  def select2(queries: Seq[thrift.SelectQuery]) = {
    edges.select(queries map { selectQueryFromThrift(_) }) map { _ map { longResultsToThrift(_) } }
  }

  def selectEdges(queries: Seq[thrift.EdgeQuery]) = {
    edges.selectEdges(queries map { edgeQueryFromThrift(_) }) map { _ map { edgeResultsToThrift(_) } }
  }

  def execute(operations: thrift.ExecuteOperations) = {
    edges.execute(executeOpsFromThrift(operations))
  }

  @deprecated("Use `count2` instead")
  def count(query: Seq[thrift.SelectOperation]) = {
    edges.count(List(query map { selectOpFromThrift(_) })) map { _.head }
  }

  def count2(queries: Seq[Seq[thrift.SelectOperation]]) = {
    edges.count(queries map { _ map { selectOpFromThrift(_) } }) map { packInts(_) }
  }

  // conversions.
  // TODO: these may be able to be removed, but the prevalence of using byte arrays in the current API makes this difficult.

  private def queryTermFromThrift(t: thrift.QueryTerm) = QueryTerm(
    t.sourceId,
    t.graphId,
    t.isForward,
    t.destinationIds map { unpackLongs(_) },
    (t.stateIds getOrElse Nil) map { State(_) }
  )

  private def pageFromThrift(p: thrift.Page) = {
    Page(p.count, Cursor(p.cursor))
  }

  private def selectQueryFromThrift(q: thrift.SelectQuery) = {
    SelectQuery(q.operations map { selectOpFromThrift(_) }, pageFromThrift(q.page))
  }

  private def selectOpFromThrift(op: thrift.SelectOperation) = {
    SelectOperation(SelectOperationType(op.operationType.value), op.term map { queryTermFromThrift(_) })
  }

  private def edgeQueryFromThrift(q: thrift.EdgeQuery) = {
    EdgeQuery(queryTermFromThrift(q.term), pageFromThrift(q.page))
  }

  private def executeOpsFromThrift(eo: thrift.ExecuteOperations) = {
    val ops = eo.operations map { o =>
      ExecuteOperation(ExecuteOperationType(o.operationType.value), queryTermFromThrift(o.term), o.position)
    }

    ExecuteOperations(ops, eo.executeAt, Priority(eo.priority.value))
  }

  private def edgeToThrift(e: Edge) = thrift.Edge(
    e.sourceId,
    e.destinationId,
    e.position,
    e.updatedAtSeconds,
    e.count,
    e.state.id
  )

  private def metadataToThrift(m: Metadata) = thrift.Metadata(
    m.sourceId,
    m.state.id,
    m.count,
    m.updatedAtSeconds
  )

  private def edgeResultsToThrift(r: ResultWindow[Edge]) = {
    thrift.EdgeResults(r.toList map { edgeToThrift(_) }, r.nextCursor.position, r.prevCursor.position)
  }

  private def longResultsToThrift(r: ResultWindow[Long]) = {
    thrift.Results(packLongs(r.toList), r.nextCursor.position, r.prevCursor.position)
  }

  private def packBuffer(size: Int)(f: ByteBuffer => Unit) = {
    val buffer = new Array[Byte](size)
    val byteBuffer = ByteBuffer.wrap(buffer)
    byteBuffer.order(ByteOrder.LITTLE_ENDIAN)
    f(byteBuffer)
    byteBuffer.rewind
    byteBuffer
  }

  private def packLongs(ls: Seq[Long]) = packBuffer(ls.size * 8) { b => ls foreach { b.putLong(_) } }

  private def packInts(is: Seq[Int]) = packBuffer(is.size * 4) { b => is foreach { b.putInt(_) } }

  private def unpackLongs(b: ByteBuffer) = {
    b.order(ByteOrder.LITTLE_ENDIAN)

    val longs   = b.asLongBuffer
    val results = new Array[Long](longs.limit)

    longs.get(results)
    results
  }
}
