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
  def schedule(tableId: Int, forwardingManager: ForwardingManager, scheduler: PrioritizingJobScheduler[JsonJob], priority: Int): Unit
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
  def apply(shardIds: Seq[ShardId]) = {
    new MetadataRepair(shardIds, MetadataRepair.START, MetadataRepair.COUNT, nameServer, scheduler)
  }
}

class RepairParser(nameServer: NameServer[Shard], scheduler: PrioritizingJobScheduler[JsonJob])
      extends RepairJobParser[Shard] {
  def deserialize(attributes: Map[String, Any], shardIds: Seq[ShardId], count: Int) = {
    val cursor = (Cursor(attributes("cursor1").asInstanceOf[AnyVal].toLong),
                    Cursor(attributes("cursor2").asInstanceOf[AnyVal].toLong))
    new Repair(shardIds, cursor, count, nameServer, scheduler)
  }
}

abstract class MultiShardRepair[R <: Repairable[R], C <: Any](shardIds: Seq[ShardId], cursor: C, count: Int,
    nameServer: NameServer[Shard], scheduler: PrioritizingJobScheduler[JsonJob]) extends RepairJob(shardIds, count, nameServer, scheduler, Repair.PRIORITY) {

  def scheduleNextRepair(lowestItem: Option[R]): Unit

  def enqueueFirst(tableId: Int, list:ListBuffer[R]) = {
    val item = list.remove(0)
    item.schedule(tableId, forwardingManager, scheduler, priority)
  }

  def forwardingManager = new ForwardingManager(nameServer)

  def cursorAtEnd(cursor: C): Boolean

  def repairListCursor(listCursors: Seq[(ListBuffer[R], C)], tableIds: Seq[Int]) = {
    if (tableIds.forall((id) => id == tableIds(0))) {
      while (listCursors.exists(lc => !lc._1.isEmpty || cursorAtEnd(lc._2)) && listCursors.exists(lc => !lc._1.isEmpty)) {
        val tableId = tableIds(0)
        val lists = scala.util.Sorting.stableSort(listCursors.map(_._1).filter(!_.isEmpty), (e1:ListBuffer[R], e2:ListBuffer[R]) => e1.first.similar(e2.first) < 0)
        val firstItem = lists(0).remove(0)
        var firstEnqueued = false
        val similarLists = lists.slice(1, lists.size).filter(_(0).similar(firstItem) == 0)
        if (similarLists.size != lists.size) {
          firstEnqueued = true
          firstItem.schedule(tableId, forwardingManager, scheduler, priority)
        }
        for (list <- similarLists) {
          if (firstItem == list(0)) {
            list.remove(0)
          } else {
            if (!firstEnqueued) {
              firstEnqueued = true
              firstItem.schedule(tableId, forwardingManager, scheduler, priority)
            }
            enqueueFirst(tableId, list)
          }
        }
      }
      val lists = scala.util.Sorting.stableSort(listCursors.map(_._1).filter(!_.isEmpty), (e1:ListBuffer[R], e2:ListBuffer[R]) => e1.first.similar(e2.first) < 0)
      scheduleNextRepair(if (lists.isEmpty) None else lists(0).firstOption)
    } else {
      throw new RuntimeException("tableIds didn't match")
    }
    
  }
}

class Repair(shardIds: Seq[ShardId], cursor: Repair.RepairCursor, count: Int,
    nameServer: NameServer[Shard], scheduler: PrioritizingJobScheduler[JsonJob])
      extends MultiShardRepair[Edge, Repair.RepairCursor](shardIds, cursor, count, nameServer, scheduler) {

  private val log = Logger.get(getClass.getName)

  def generateCursor(edge: Edge) = {
    (Cursor(edge.sourceId), Cursor(edge.destinationId)) 
  }

  def cursorAtEnd(c: Repair.RepairCursor) = c == Repair.END

  def repair(shards: Seq[Shard]) = {
    val tableIds = shards.map((shard:Shard) => nameServer.getRootForwardings(shard.shardInfo.id)(0).tableId)

    val listCursors = shards.map( (shard) => {
      val (seq, newCursor) = shard.selectAll(cursor, count)
      val list = new ListBuffer[Edge]()
      list ++= seq
      (list, newCursor)
    })
    repairListCursor(listCursors, tableIds)
  }

  def serialize = Map("cursor1" -> cursor._1.position, "cursor2" -> cursor._2.position)

  def scheduleNextRepair(lowestEdge: Option[Edge]) = {
    lowestEdge match {
      case None => None
      case _ => scheduler.put(Repair.PRIORITY, new Repair(shardIds, generateCursor(lowestEdge.get), count, nameServer, scheduler))
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
  def deserialize(attributes: Map[String, Any], shardIds: Seq[ShardId], count: Int) = {
    val cursor  = Cursor(attributes("cursor").asInstanceOf[AnyVal].toLong)
    new MetadataRepair(shardIds, cursor, count, nameServer, scheduler)
  }
}

class MetadataRepair(shardIds: Seq[ShardId], cursor: MetadataRepair.RepairCursor, count: Int,
    nameServer: NameServer[Shard], scheduler: PrioritizingJobScheduler[JsonJob])
  extends MultiShardRepair[Metadata, MetadataRepair.RepairCursor](shardIds, cursor, count, nameServer, scheduler) {

  private val log = Logger.get(getClass.getName)

  def scheduleNextRepair(lowestMetadata: Option[Metadata]) = {
    scheduler.put(Repair.PRIORITY, lowestMetadata match {
      case None => new Repair(shardIds, Repair.START, Repair.COUNT, nameServer, scheduler)
      case _ => new MetadataRepair(shardIds, generateCursor(lowestMetadata.get), count, nameServer, scheduler)
    })
  }

  def cursorAtEnd(c: MetadataRepair.RepairCursor) = c == MetadataRepair.END

  def generateCursor(metadata: Metadata) = {
    Cursor(metadata.sourceId)
  }

  def repair(shards: Seq[Shard]) = {
    val tableIds = shards.map((shard:Shard) => nameServer.getRootForwardings(shard.shardInfo.id)(0).tableId)

    val listCursors = shards.map( (shard) => {
      val (seq, newCursor) = shard.selectAllMetadata(cursor, count)
      val list = new ListBuffer[Metadata]()
      list ++= seq
      (list, newCursor)
    })
    repairListCursor(listCursors, tableIds)
  }

  def serialize = Map("cursor" -> cursor.position)
}
