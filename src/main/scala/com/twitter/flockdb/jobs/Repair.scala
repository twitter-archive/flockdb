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
package jobs

import com.twitter.gizzard.scheduler._
import com.twitter.gizzard.shards.ShardId
import com.twitter.gizzard.nameserver.NameServer
import com.twitter.gizzard.nameserver.{NameServer, NonExistentShard}
import com.twitter.gizzard.shards.{ShardDatabaseTimeoutException, ShardTimeoutException}
import com.twitter.logging.Logger
import com.twitter.ostrich.stats.Stats
import com.twitter.util.TimeConversions._
import collection.mutable.ListBuffer
import conversions.Numeric._
import shards.{Shard}

object Repair {
  type RepairCursor = (Cursor, Cursor)
  val START = (Cursor.Start, Cursor.Start)
  val END = (Cursor.End, Cursor.End)
  val COUNT = 10000
  val PRIORITY = Priority.Medium.id
}

class RepairFactory(nameServer: NameServer[Shard], scheduler: PrioritizingJobScheduler)
      extends RepairJobFactory[Shard] {
  def apply(shardIds: Seq[ShardId]) = {
    new MetadataRepair(shardIds, MetadataRepair.START, MetadataRepair.COUNT, nameServer, scheduler)
  }
}

class RepairParser(nameServer: NameServer[Shard], scheduler: PrioritizingJobScheduler)
      extends RepairJobParser[Shard] {
  def deserialize(attributes: Map[String, Any], shardIds: Seq[ShardId], count: Int) = {
    val cursor = (Cursor(attributes("cursor1").asInstanceOf[AnyVal].toLong),
                    Cursor(attributes("cursor2").asInstanceOf[AnyVal].toLong))
    new Repair(shardIds, cursor, count, nameServer, scheduler)
  }
}

class Repair(shardIds: Seq[ShardId], cursor: Repair.RepairCursor, count: Int,
    nameServer: NameServer[Shard], scheduler: PrioritizingJobScheduler)
  extends MultiShardRepair[Shard, Edge, Repair.RepairCursor](shardIds, cursor, count, nameServer, scheduler, Repair.PRIORITY) {

  private val log = Logger.get(getClass.getName)

  override def label = "Repair"

  def cursorAtEnd(c: Repair.RepairCursor) = c == Repair.END

  def lowestCursor(c1: Repair.RepairCursor, c2: Repair.RepairCursor) = {
    (c1, c2) match {
      case (Repair.END, Repair.END) => c1
      case (_, Repair.END) => c1
      case (Repair.END, _) => c2
      case (_, _) =>
        c1._1.position.compare(c2._1.position) match {
          case x if x < 0 => c1
          case x if x > 0 => c2
          case _ =>
            c1._2.position.compare(c2._2.position) match {
              case x if x < 0 => c1
              case x if x > 0 => c2
              case _ => c1
            }
        }
    }
  }

  def forwardingManager = new ForwardingManager(nameServer)

  def scheduleBulk(otherShards: Seq[Shard], items: Seq[Edge]) = {
    otherShards.foreach(_.writeCopies(items))
  }

  def scheduleDifferent(list: (Shard, ListBuffer[Edge], Repair.RepairCursor), tableId: Int, item: Edge) = {
    item.schedule(tableId, forwardingManager, scheduler, priority)
  }

  def scheduleMissing(list: (Shard, ListBuffer[Edge], Repair.RepairCursor), tableId: Int, item: Edge) = {
    item.schedule(tableId, forwardingManager, scheduler, priority)
  }

  def shouldSchedule(original: Edge, suspect: Edge) = {
    original.sourceId != suspect.sourceId || original.destinationId != suspect.destinationId || original.updatedAt != suspect.updatedAt || original.state != suspect.state
  }


  def repair(shards: Seq[Shard]) = {
    val tableIds = shards.map(shard => nameServer.getRootForwardings(shard.shardInfo.id).head.tableId)

    val listCursors = shards.map( (shard) => {
      val (seq, newCursor) = shard.selectAll(cursor, count)
      val list = new ListBuffer[Edge]()
      list ++= seq
      (shard, list, newCursor)
    })
    repairListCursor(listCursors, tableIds)
  }

  def serialize = Map("cursor1" -> cursor._1.position, "cursor2" -> cursor._2.position)

  def nextRepair(lowestCursor: Repair.RepairCursor) = {
    lowestCursor match {
      case Repair.END => None
      case _ => Some(new Repair(shardIds, lowestCursor, count, nameServer, scheduler))
    }
  }
}

object MetadataRepair {
  type RepairCursor = Cursor
  val START = Cursor.Start
  val END = Cursor.End
  val COUNT = 10000
  val PRIORITY = Priority.Medium.id
}

class MetadataRepairParser(nameServer: NameServer[Shard], scheduler: PrioritizingJobScheduler)
      extends RepairJobParser[Shard] {
  def deserialize(attributes: Map[String, Any], shardIds: Seq[ShardId], count: Int) = {
    val cursor  = Cursor(attributes("cursor").asInstanceOf[AnyVal].toLong)
    new MetadataRepair(shardIds, cursor, count, nameServer, scheduler)
  }
}

class MetadataRepair(shardIds: Seq[ShardId], cursor: MetadataRepair.RepairCursor, count: Int,
    nameServer: NameServer[Shard], scheduler: PrioritizingJobScheduler)
  extends MultiShardRepair[Shard, Metadata, MetadataRepair.RepairCursor](shardIds, cursor, count, nameServer, scheduler, Repair.PRIORITY) {

  private val log = Logger.get(getClass.getName)

  override def label = "MetadataRepair"

  def nextRepair(lowestCursor: MetadataRepair.RepairCursor) = {
    Some(lowestCursor match {
      case MetadataRepair.END => new Repair(shardIds, Repair.START, Repair.COUNT, nameServer, scheduler)
      case _ => new MetadataRepair(shardIds, lowestCursor, count, nameServer, scheduler)
    })
  }

  def scheduleDifferent(list: (Shard, ListBuffer[Metadata], MetadataRepair.RepairCursor), tableId: Int, item: Metadata) = {
    item.schedule(tableId, forwardingManager, scheduler, priority)
  }

  def scheduleMissing(list: (Shard, ListBuffer[Metadata], MetadataRepair.RepairCursor), tableId: Int, item: Metadata) = {
    if (item.state != State.Normal || item.count != 0) item.schedule(tableId, forwardingManager, scheduler, priority)
  }

  def scheduleBulk(otherShards: Seq[Shard], items: Seq[Metadata]) = {
    otherShards.foreach(_.writeMetadata(items))
  }

  def forwardingManager = new ForwardingManager(nameServer)

  def cursorAtEnd(c: MetadataRepair.RepairCursor) = c == MetadataRepair.END

  def shouldSchedule(original: Metadata, suspect: Metadata) = {
    original.sourceId != suspect.sourceId || original.updatedAt != suspect.updatedAt || original.state != suspect.state
  }

  def lowestCursor(c1: MetadataRepair.RepairCursor, c2: MetadataRepair.RepairCursor) = {
    (c1, c2) match {
      case (MetadataRepair.END, MetadataRepair.END) => c1
      case (_, MetadataRepair.END) => c1
      case (MetadataRepair.END, _) => c2
      case (_, _) =>
        c1.position.compare(c2.position) match {
          case x if x < 0 => c1
          case x if x > 0 => c2
          case _ => c1
        }
    }
  }

  def repair(shards: Seq[Shard]) = {
    val tableIds = shards.map(shard => nameServer.getRootForwardings(shard.shardInfo.id).head.tableId)

    val listCursors = shards.map( (shard) => {
      val (seq, newCursor) = shard.selectAllMetadata(cursor, count)
      val list = new ListBuffer[Metadata]()
      list ++= seq
      (shard, list, newCursor)
    })
    repairListCursor(listCursors, tableIds)
  }

  def serialize = Map("cursor" -> cursor.position)
}
