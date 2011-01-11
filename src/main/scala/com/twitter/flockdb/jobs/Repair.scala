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

package com.twitter.flockdb.jobs

import com.twitter.gizzard.scheduler._
import com.twitter.gizzard.shards.ShardId
import com.twitter.gizzard.nameserver.NameServer
import com.twitter.ostrich.Stats
import com.twitter.util.TimeConversions._
import conversions.Numeric._
import shards.{Shard, Metadata}
import net.lag.logging.Logger
import com.twitter.gizzard.nameserver.{NameServer, NonExistentShard}
import com.twitter.gizzard.shards.{ShardDatabaseTimeoutException, ShardTimeoutException}
import collection.mutable.ListBuffer

object RepairJob {
  val MIN_COPY = 500
  val PRIORITY = Priority.Low.id
}

/**
 * A factory for creating a new repair job (with default count and a starting cursor) from a source
 * and destination shard ID.
 */
trait RepairJobFactory[S <: Shard, R <: Repairable[R]] extends ((ShardId, ShardId, Int) => RepairJob[S, R])

/**
 * A parser that creates a repair job out of json. The basic attributes (source shard ID, destination)
 * shard ID, count and dry run) are parsed out first, and the remaining attributes are passed to
 * 'deserialize' to decode any shard-specific data (like a cursor).
 */
trait RepairJobParser[S <: Shard, R <: Repairable[R]] extends JsonJobParser {
  def deserialize(attributes: Map[String, Any], sourceId: ShardId, destinationId: ShardId, count: Int, tableId: Int, dryRun: Boolean): RepairJob[S, R]

  def apply(attributes: Map[String, Any]): JsonJob = {
    deserialize(attributes,
      ShardId(attributes("source_shard_hostname").toString, attributes("source_shard_table_prefix").toString),
      ShardId(attributes("destination_shard_hostname").toString, attributes("destination_shard_table_prefix").toString),
      attributes("count").asInstanceOf[{def toInt: Int}].toInt, attributes("table_id").asInstanceOf[AnyVal].toInt,
      attributes("dry_run").asInstanceOf[Boolean])
  }
}

/**
 * A json-encodable job that represents the state of a repair one a shard.
 *
 * The 'toMap' implementation encodes the source and destination shard IDs, dry run state, and the count of items.
 * Other shard-specific data (like the cursor) can be encoded in 'serialize'.
 *
 * 'repair' is called to do the actual data repair. It should return a new Some[RepairJob] representing
 * the next chunk of work to do, or None if the entire copying job is complete.
 */
abstract case class RepairJob[S <: Shard, R <: Repairable[R]](sourceId: ShardId,
                                       destinationId: ShardId,
                                       tableId: Int,
                                       var count: Int,
                                       nameServer: NameServer[S],
                                       scheduler: PrioritizingJobScheduler[JsonJob],
                                       dryRun: Boolean) extends JsonJob {
  private val log = Logger.get(getClass.getName)

  def finish() {
    log.info("Repair finished for (type %s) from %s to %s",
             getClass.getName.split("\\.").last, sourceId, destinationId)
    Stats.clearGauge(gaugeName)
  }

  def apply() {
    try {
      log.info("Repairing shard block (type %s) from %s to %s: state=%s",
               getClass.getName.split("\\.").last, sourceId, destinationId, toMap)
      val sourceShard = nameServer.findShardById(sourceId)
      val destinationShard = nameServer.findShardById(destinationId)
      repair(sourceShard, destinationShard)
    } catch {
      case e: NonExistentShard =>
        log.error("Shard block repair failed because one of the shards doesn't exist. Terminating the repair.")
      case e: ShardDatabaseTimeoutException =>
        log.warning("Shard block repair failed to get a database connection; retrying.")
        scheduler.put(RepairJob.PRIORITY, this)
      case e: ShardTimeoutException if (count > RepairJob.MIN_COPY) =>
        log.warning("Shard block copy timed out; trying a smaller block size.")
        count = (count * 0.9).toInt
        scheduler.put(RepairJob.PRIORITY, this)
      case e: Throwable =>
        log.warning("Shard block repair stopped due to exception: %s", e)
        throw e
    }
  }

  def toMap = {
    Map("source_shard_hostname" -> sourceId.hostname,
        "source_shard_table_prefix" -> sourceId.tablePrefix,
        "destination_shard_hostname" -> destinationId.hostname,
        "destination_shard_table_prefix" -> destinationId.tablePrefix,
        "table_id" -> tableId,
        "dry_run" -> dryRun,
        "count" -> count
    ) ++ serialize
  }

  def incrGauge = {
    Stats.setGauge(gaugeName, Stats.getGauge(gaugeName).getOrElse(0.0) + 1)
  }

  private def gaugeName = {
    "x-repairing-" + sourceId + "-" + destinationId
  }

  def repair(sourceShard: S, destinationShard: S)

  def serialize: Map[String, Any]
  
  def enqueueFirst(list: ListBuffer[R])
  
  def resolve(srcSeq: Seq[R], srcCursorAtEnd: Boolean, destSeq: Seq[R], destCursorAtEnd: Boolean) = {
    val srcItems = new ListBuffer[R]()
    srcItems ++= srcSeq
    val destItems = new ListBuffer[R]()
    destItems ++= destSeq
    var running = !(srcItems.isEmpty && destItems.isEmpty)
    while (running) {
      val srcEdge = srcItems.firstOption
      val destEdge = destItems.firstOption
      (srcCursorAtEnd, destCursorAtEnd, srcEdge, destEdge) match {
        case (true, true, None, None) => running = false
        case (true, true, _, None) => enqueueFirst(srcItems)
        case (true, true, None, _) => enqueueFirst(destItems)
        case (true, _, _, _) => running = false
        case (_, true, _, _) => running = false
        case (_, _, None, None) => running = false
        case (_, _, _, None) => running = false
        case (_, _, None, _) => running = false
        case (_, _, _, _) =>
          srcEdge.get.similar(destEdge.get) match {
            case x if x < 0 => enqueueFirst(srcItems)
            case x if x > 0 => enqueueFirst(destItems)
            case _ =>
              if (srcEdge != destEdge) {
                enqueueFirst(srcItems)
                enqueueFirst(destItems)
              } else {
                srcItems.remove(0)
                destItems.remove(0)
              }
          }
      }
      running &&= !(srcItems.isEmpty && destItems.isEmpty)
    }
    (srcItems.firstOption, destItems.firstOption)
  }

}

object Repair {
  type RepairCursor = (Cursor, Cursor)
  val START = (Cursor.Start, Cursor.Start)
  val END = (Cursor.End, Cursor.End)
  val COUNT = 10000
}

class RepairFactory(nameServer: NameServer[Shard], scheduler: PrioritizingJobScheduler[JsonJob], dryRun: Boolean)
      extends RepairJobFactory[Shard, Metadata] {
  def apply(sourceShardId: ShardId, destShardId: ShardId, tableId: Int) = 
    new MetadataRepair(sourceShardId, destShardId, tableId, MetadataRepair.START, MetadataRepair.START, MetadataRepair.COUNT, nameServer, scheduler, dryRun)
}

class RepairParser(nameServer: NameServer[Shard], scheduler: PrioritizingJobScheduler[JsonJob])
      extends RepairJobParser[Shard, Edge] {
  def deserialize(attributes: Map[String, Any], sourceId: ShardId, destinationId: ShardId, count: Int, tableId: Int, dryRun: Boolean) = {
    val srcCursor = (Cursor(attributes("src_cursor1").asInstanceOf[AnyVal].toLong),
                    Cursor(attributes("src_cursor2").asInstanceOf[AnyVal].toLong))
    val destCursor = (Cursor(attributes("dest_cursor1").asInstanceOf[AnyVal].toLong),
                    Cursor(attributes("dest_cursor2").asInstanceOf[AnyVal].toLong))
    new Repair(sourceId, destinationId, tableId, srcCursor, destCursor, count, nameServer, scheduler, dryRun)
  }
}

class Repair(sourceShardId: ShardId, destinationShardId: ShardId, tableId: Int, srcCursor: Repair.RepairCursor,
           destCursor: Repair.RepairCursor, count: Int, nameServer: NameServer[Shard], scheduler: PrioritizingJobScheduler[JsonJob], dryRun: Boolean)
      extends RepairJob[Shard, Edge](sourceShardId, destinationShardId, tableId, count, nameServer, scheduler, dryRun) {

  private val log = Logger.get(getClass.getName)

  def generateCursor(edge: Edge) = {
    (Cursor(edge.sourceId), Cursor(edge.destinationId)) 
  }

  def repair(sourceShard: Shard, destinationShard: Shard) = {
    val (srcSeq,  newSrcCursor) = sourceShard.selectAll(srcCursor, count)
    val (destSeq, newDestCursor) = destinationShard.selectAll(destCursor, count)
    val (srcEdge, destEdge) = resolve(srcSeq, newSrcCursor == Repair.END, destSeq, newDestCursor == Repair.END)
    scheduleNextRepair(srcEdge, newSrcCursor, destEdge, newDestCursor)
  }

  def enqueueFirst(list:ListBuffer[Edge]) = {
    val edge = list.remove(0)
    if (dryRun) log.info("Enqueuing edge (sourceId: %s, destinationId: %s, state: %s)", edge.sourceId, edge.destinationId, edge.state)
    else edge.schedule(tableId, forwardingManager, scheduler, RepairJob.PRIORITY)
  }

  def forwardingManager = new ForwardingManager(nameServer)

  def serialize = Map("src_cursor1" -> srcCursor._1.position, "src_cursor2" -> srcCursor._2.position, "dest_cursor1" -> destCursor._1.position, "dest_cursor2" -> destCursor._2.position, "table_id" -> tableId)

  def scheduleNextRepair(srcEdge: Option[Edge], newSrcCursor: Repair.RepairCursor, destEdge: Option[Edge], newDestCursor: Repair.RepairCursor) = {
    (newSrcCursor, newDestCursor) match {
      case (Repair.END, Repair.END) => finish()
      case (_, _) => 
        incrGauge
        scheduler.put(RepairJob.PRIORITY, (srcEdge, destEdge) match {
          case (None, None) =>
            new Repair(sourceShardId, destinationShardId, tableId, newSrcCursor, newDestCursor, count, nameServer, scheduler, dryRun)
          case (_, None) => 
            new Repair(sourceShardId, destinationShardId, tableId, generateCursor(srcEdge.get), generateCursor(srcEdge.get), count, nameServer, scheduler, dryRun)
          case (None, _) => 
            new Repair(sourceShardId, destinationShardId, tableId, generateCursor(destEdge.get), generateCursor(destEdge.get), count, nameServer, scheduler, dryRun)
          case (_, _) => 
            var newCursor = generateCursor(if (srcEdge.get.similar(destEdge.get) <= 0) srcEdge.get else destEdge.get)
            new Repair(sourceShardId, destinationShardId, tableId, newCursor, newCursor, count, nameServer, scheduler, dryRun)
        })
    }
  }
}

object MetadataRepair {
  type RepairCursor = Cursor
  val START = Cursor.Start
  val END = Cursor.End
  val COUNT = 10000
}

class MetadataRepairParser(nameServer: NameServer[Shard], scheduler: PrioritizingJobScheduler[JsonJob])
      extends RepairJobParser[Shard, Metadata] {
  def deserialize(attributes: Map[String, Any], sourceId: ShardId, destinationId: ShardId, count: Int, tableId: Int, dryRun: Boolean) = {
    val srcCursor  = Cursor(attributes("src_cursor").asInstanceOf[AnyVal].toLong)
    val destCursor = Cursor(attributes("dest_cursor").asInstanceOf[AnyVal].toLong)
    new MetadataRepair(sourceId, destinationId, tableId, srcCursor, destCursor, count, nameServer, scheduler, dryRun)
  }
}

class MetadataRepair(sourceShardId: ShardId, destinationShardId: ShardId, tableId: Int, srcCursor: MetadataRepair.RepairCursor,
     destCursor: MetadataRepair.RepairCursor, count: Int, nameServer: NameServer[Shard], scheduler: PrioritizingJobScheduler[JsonJob], dryRun: Boolean)
      extends RepairJob[Shard, Metadata](sourceShardId, destinationShardId, tableId, count, nameServer, scheduler, dryRun) {

  private val log = Logger.get(getClass.getName)

  def scheduleNextRepair(srcEdge: Option[Metadata], newSrcCursor: MetadataRepair.RepairCursor, destEdge: Option[Metadata], newDestCursor: MetadataRepair.RepairCursor) = {
    scheduler.put(RepairJob.PRIORITY, (newSrcCursor, newDestCursor) match {
      case (MetadataRepair.END, MetadataRepair.END) => new Repair(sourceShardId, destinationShardId, tableId, Repair.START, Repair.START, Repair.COUNT, nameServer, scheduler, dryRun)
      case (_, _) => 
        incrGauge
        (srcEdge, destEdge) match {
          case (None, None) =>
            new MetadataRepair(sourceShardId, destinationShardId, tableId, newSrcCursor, newDestCursor, count, nameServer, scheduler, dryRun)
          case (_, None) => 
            new MetadataRepair(sourceShardId, destinationShardId, tableId, generateCursor(srcEdge.get), generateCursor(srcEdge.get), count, nameServer, scheduler, dryRun)
          case (None, _) => 
            new MetadataRepair(sourceShardId, destinationShardId, tableId, generateCursor(destEdge.get), generateCursor(destEdge.get), count, nameServer, scheduler, dryRun)
          case (_, _) => 
            var newCursor = generateCursor(if (srcEdge.get.sourceId <= destEdge.get.sourceId) srcEdge.get else destEdge.get)
            new MetadataRepair(sourceShardId, destinationShardId, tableId, newCursor, newCursor, count, nameServer, scheduler, dryRun)
        }
    })
  }

  def generateCursor(metadata: Metadata) = {
    Cursor(metadata.sourceId)
  }

  def forwardingManager = new ForwardingManager(nameServer)

  def enqueueFirst(list:ListBuffer[Metadata]) = {
    val metadata = list.remove(0)
    if (dryRun) log.info("Enqueuing edge (sourceId: %s, state: %s)", metadata.sourceId, metadata.state)
    else metadata.schedule(tableId, forwardingManager, scheduler, RepairJob.PRIORITY)
  }

  def repair(sourceShard: Shard, destinationShard: Shard) = {
    val (srcSeq,  newSrcCursor) = sourceShard.selectAllMetadata(srcCursor, count)
    val (destSeq, newDestCursor) = destinationShard.selectAllMetadata(destCursor, count)
    val (srcMetadata, destMetadata) = resolve(srcSeq, newSrcCursor == MetadataRepair.END, destSeq, newDestCursor == MetadataRepair.END)
    scheduleNextRepair(srcMetadata, newSrcCursor, destMetadata, newDestCursor)
  }

  def serialize = Map("src_cursor" -> srcCursor.position, "dest_cursor" -> destCursor.position, "table_id" -> tableId)
}
