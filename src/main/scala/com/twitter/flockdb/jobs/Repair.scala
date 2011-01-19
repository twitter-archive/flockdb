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

trait Repairable[T] {
  def similar(other: T): Int
}

object Repair {
  type RepairCursor = (Cursor, Cursor)
  val START = (Cursor.Start, Cursor.Start)
  val END = (Cursor.End, Cursor.End)
  val COUNT = 10000
  val PRIORITY = Priority.Low.id
}

class RepairFactory(nameServer: NameServer[Shard], scheduler: PrioritizingJobScheduler[JsonJob])
      extends RepairJobFactory[Shard] {
  def apply(sourceShardId: ShardId, destShardId: ShardId) = {
    new MetadataRepair(sourceShardId, destShardId, MetadataRepair.START, MetadataRepair.START, MetadataRepair.COUNT, nameServer, scheduler)
  }
}

class RepairParser(nameServer: NameServer[Shard], scheduler: PrioritizingJobScheduler[JsonJob])
      extends RepairJobParser[Shard] {
  def deserialize(attributes: Map[String, Any], sourceId: ShardId, destinationId: ShardId, count: Int) = {
    val srcCursor = (Cursor(attributes("src_cursor1").asInstanceOf[AnyVal].toLong),
                    Cursor(attributes("src_cursor2").asInstanceOf[AnyVal].toLong))
    val destCursor = (Cursor(attributes("dest_cursor1").asInstanceOf[AnyVal].toLong),
                    Cursor(attributes("dest_cursor2").asInstanceOf[AnyVal].toLong))
    new Repair(sourceId, destinationId, srcCursor, destCursor, count, nameServer, scheduler)
  }
}

abstract class TwoCursorRepair[R <: Repairable[R]](sourceShardId: ShardId, destinationShardId: ShardId, count: Int, nameServer: NameServer[Shard], scheduler: PrioritizingJobScheduler[JsonJob], priority: Int) extends RepairJob[Shard](sourceShardId, destinationShardId, count, nameServer, scheduler, priority) {
  
  def enqueueFirst(tableId: Int, list: ListBuffer[R])
  
  def resolve(tableId: Int, srcSeq: Seq[R], srcCursorAtEnd: Boolean, destSeq: Seq[R], destCursorAtEnd: Boolean) = {
    val srcItems = new ListBuffer[R]()
    srcItems ++= srcSeq
    val destItems = new ListBuffer[R]()
    destItems ++= destSeq
    var running = !(srcItems.isEmpty && destItems.isEmpty)
    while (running) {
      val srcItem = srcItems.firstOption
      val destItem = destItems.firstOption
      (srcCursorAtEnd, destCursorAtEnd, srcItem, destItem) match {
        case (true, true, None, None) => running = false
        case (true, true, _, None) => enqueueFirst(tableId, srcItems)
        case (true, true, None, _) => enqueueFirst(tableId, destItems)
        case (_, _, _, _) =>
          (srcItem, destItem) match {
            case (None, None) => running = false
            case (_, None) => running = false
            case (None, _) => running = false
            case (_, _) =>
              srcItem.get.similar(destItem.get) match {
                case x if x < 0 => enqueueFirst(tableId, srcItems)
                case x if x > 0 => enqueueFirst(tableId, destItems)
                case _ =>
                  if (srcItem != destItem) {
                    enqueueFirst(tableId, srcItems)
                    enqueueFirst(tableId, destItems)
                  } else {
                    srcItems.remove(0)
                    destItems.remove(0)
                  }
              }
          }
      }
      running &&= !(srcItems.isEmpty && destItems.isEmpty)
    }
    (srcItems.firstOption, destItems.firstOption)
  }
  
}

class Repair(sourceShardId: ShardId, destinationShardId: ShardId, srcCursor: Repair.RepairCursor,
           destCursor: Repair.RepairCursor, count: Int, nameServer: NameServer[Shard], 
           scheduler: PrioritizingJobScheduler[JsonJob])
      extends TwoCursorRepair[Edge](sourceShardId, destinationShardId, count, nameServer, scheduler, Repair.PRIORITY) {

  private val log = Logger.get(getClass.getName)

  def generateCursor(edge: Edge) = {
    (Cursor(edge.sourceId), Cursor(edge.destinationId)) 
  }

  def repair(sourceShard: Shard, destinationShard: Shard) = {
    val (srcSeq,  newSrcCursor) = sourceShard.selectAll(srcCursor, count)
    val (destSeq, newDestCursor) = destinationShard.selectAll(destCursor, count)
    val sourceTableId = nameServer.getRootForwardings(sourceShard.shardInfo.id)(0).tableId
    val destinationTableId = nameServer.getRootForwardings(destinationShard.shardInfo.id)(0).tableId
    if (sourceTableId != destinationTableId) {
      throw new RuntimeException(sourceShard+" tableId did not match "+destinationShard);
    } else {
      val (srcEdge, destEdge) = resolve(sourceTableId, srcSeq, newSrcCursor == Repair.END, destSeq, newDestCursor == Repair.END)
      scheduleNextRepair(srcEdge, newSrcCursor, destEdge, newDestCursor)
    }
  }

  def enqueueFirst(tableId: Int, list:ListBuffer[Edge]) = {
    val edge = list.remove(0)
    edge.schedule(tableId, forwardingManager, scheduler, priority)
  }

  def forwardingManager = new ForwardingManager(nameServer)

  def serialize = Map("src_cursor1" -> srcCursor._1.position, "src_cursor2" -> srcCursor._2.position, "dest_cursor1" -> destCursor._1.position, "dest_cursor2" -> destCursor._2.position)

  def scheduleNextRepair(srcEdge: Option[Edge], newSrcCursor: Repair.RepairCursor, destEdge: Option[Edge], newDestCursor: Repair.RepairCursor) = {
    (newSrcCursor, newDestCursor) match {
      case (Repair.END, Repair.END) => finish()
      case (_, _) => 
        incrGauge
        scheduler.put(Repair.PRIORITY, (srcEdge, destEdge) match {
          case (None, None) =>
            new Repair(sourceShardId, destinationShardId, newSrcCursor, newDestCursor, count, nameServer, scheduler)
          case (_, None) => 
            new Repair(sourceShardId, destinationShardId, generateCursor(srcEdge.get), generateCursor(srcEdge.get), count, nameServer, scheduler)
          case (None, _) => 
            new Repair(sourceShardId, destinationShardId, generateCursor(destEdge.get), generateCursor(destEdge.get), count, nameServer, scheduler)
          case (_, _) => 
            var newCursor = generateCursor(if (srcEdge.get.similar(destEdge.get) <= 0) srcEdge.get else destEdge.get)
            new Repair(sourceShardId, destinationShardId, newCursor, newCursor, count, nameServer, scheduler)
        })
    }
  }
}

object MetadataRepair {
  type RepairCursor = Cursor
  val START = Cursor.Start
  val END = Cursor.End
  val COUNT = 10000
  val PRIORITY = Priority.Low.id
}

class MetadataRepairParser(nameServer: NameServer[Shard], scheduler: PrioritizingJobScheduler[JsonJob])
      extends RepairJobParser[Shard] {
  def deserialize(attributes: Map[String, Any], sourceId: ShardId, destinationId: ShardId, count: Int) = {
    val srcCursor  = Cursor(attributes("src_cursor").asInstanceOf[AnyVal].toLong)
    val destCursor = Cursor(attributes("dest_cursor").asInstanceOf[AnyVal].toLong)
    new MetadataRepair(sourceId, destinationId, srcCursor, destCursor, count, nameServer, scheduler)
  }
}

class MetadataRepair(sourceShardId: ShardId, destinationShardId: ShardId, srcCursor: MetadataRepair.RepairCursor,
     destCursor: MetadataRepair.RepairCursor, count: Int, nameServer: NameServer[Shard], scheduler: PrioritizingJobScheduler[JsonJob])
  extends TwoCursorRepair[Metadata](sourceShardId, destinationShardId, count, nameServer, scheduler, MetadataRepair.PRIORITY) {

  private val log = Logger.get(getClass.getName)

  def scheduleNextRepair(srcEdge: Option[Metadata], newSrcCursor: MetadataRepair.RepairCursor, destEdge: Option[Metadata], newDestCursor: MetadataRepair.RepairCursor) = {
    scheduler.put(MetadataRepair.PRIORITY, (newSrcCursor, newDestCursor) match {
      case (MetadataRepair.END, MetadataRepair.END) =>
        new Repair(sourceShardId, destinationShardId, Repair.START, Repair.START, Repair.COUNT, nameServer, scheduler)
      case (_, _) => 
        incrGauge
        (srcEdge, destEdge) match {
          case (None, None) =>
            new MetadataRepair(sourceShardId, destinationShardId, newSrcCursor, newDestCursor, count, nameServer, scheduler)
          case (_, None) => 
            new MetadataRepair(sourceShardId, destinationShardId, generateCursor(srcEdge.get), generateCursor(srcEdge.get), count, nameServer, scheduler)
          case (None, _) => 
            new MetadataRepair(sourceShardId, destinationShardId, generateCursor(destEdge.get), generateCursor(destEdge.get), count, nameServer, scheduler)
          case (_, _) => 
            var newCursor = generateCursor(if (srcEdge.get.sourceId <= destEdge.get.sourceId) srcEdge.get else destEdge.get)
            new MetadataRepair(sourceShardId, destinationShardId, newCursor, newCursor, count, nameServer, scheduler)
        }
    })
  }

  def generateCursor(metadata: Metadata) = {
    Cursor(metadata.sourceId)
  }

  def forwardingManager = new ForwardingManager(nameServer)

  def enqueueFirst(tableId: Int, list:ListBuffer[Metadata]) = {
    val metadata = list.remove(0)
    metadata.schedule(tableId, forwardingManager, scheduler, priority)
  }

  def repair(sourceShard: Shard, destinationShard: Shard) = {
    val (srcSeq,  newSrcCursor) = sourceShard.selectAllMetadata(srcCursor, count)
    val (destSeq, newDestCursor) = destinationShard.selectAllMetadata(destCursor, count)
    val sourceTableId = nameServer.getRootForwardings(sourceShard.shardInfo.id)(0).tableId
    val destinationTableId = nameServer.getRootForwardings(destinationShard.shardInfo.id)(0).tableId
    if (sourceTableId != destinationTableId) {
      throw new RuntimeException(sourceShard+" tableId did not match "+destinationShard);
    } else {
      val (srcMetadata, destMetadata) = resolve(sourceTableId, srcSeq, newSrcCursor == MetadataRepair.END, destSeq, newDestCursor == MetadataRepair.END)
      scheduleNextRepair(srcMetadata, newSrcCursor, destMetadata, newDestCursor)
    }
  }

  def serialize = Map("src_cursor" -> srcCursor.position, "dest_cursor" -> destCursor.position)
}
