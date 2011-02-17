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
import com.twitter.ostrich.Stats
import com.twitter.util.TimeConversions._
import conversions.Numeric._
import net.lag.logging.Logger
import com.twitter.gizzard.nameserver.{NameServer, NonExistentShard}
import com.twitter.gizzard.shards.{ShardDatabaseTimeoutException, ShardTimeoutException}
import collection.mutable.ListBuffer
import shards.{Shard}

class DiffFactory(nameServer: NameServer[Shard], scheduler: PrioritizingJobScheduler[JsonJob])
      extends RepairJobFactory[Shard] {
  override def apply(shardIds: Seq[ShardId]) = {
    new MetadataDiff(shardIds, MetadataRepair.START, MetadataRepair.COUNT, nameServer, scheduler)
  }
}

class DiffParser(nameServer: NameServer[Shard], scheduler: PrioritizingJobScheduler[JsonJob])
      extends RepairParser(nameServer, scheduler) {
  override def deserialize(attributes: Map[String, Any], shardIds: Seq[ShardId], count: Int) = {
    val cursor = (Cursor(attributes("cursor1").asInstanceOf[AnyVal].toLong),
                    Cursor(attributes("cursor2").asInstanceOf[AnyVal].toLong))
    new Diff(shardIds, cursor, count, nameServer, scheduler)
  }
}

class Diff(shardIds: Seq[ShardId], cursor: Repair.RepairCursor, count: Int,
    nameServer: NameServer[Shard], scheduler: PrioritizingJobScheduler[JsonJob])
  extends Repair(shardIds, cursor, count, nameServer, scheduler) {

  private val log = Logger.get(getClass.getName)

  override def label = "Diff"

  override def scheduleMissing(list: (Shard, ListBuffer[Edge], Repair.RepairCursor), tableId: Int, item: Edge) = {
    log.info("DIFF [MISSING] -> table id:"+tableId+" shard:"+list._1.shardInfo.id+ "+edge:"+item)
  }

  override def scheduleDifferent(list: (Shard, ListBuffer[Edge], Repair.RepairCursor), tableId: Int, item: Edge) = {
    log.info("DIFF [DIFFERENT] -> table id:"+tableId+" shard:"+list._1.shardInfo.id+ "+edge:"+item)
  }

  override def scheduleNextRepair(lowestCursor: Repair.RepairCursor) = {
    lowestCursor match {
      case Repair.END => None
      case _ => scheduler.put(Repair.PRIORITY, new Diff(shardIds, lowestCursor, count, nameServer, scheduler))
    }
  }
}

class MetadataDiffParser(nameServer: NameServer[Shard], scheduler: PrioritizingJobScheduler[JsonJob])
      extends MetadataRepairParser(nameServer, scheduler) {
  override def deserialize(attributes: Map[String, Any], shardIds: Seq[ShardId], count: Int) = {
    val cursor  = Cursor(attributes("cursor").asInstanceOf[AnyVal].toLong)
    new MetadataDiff(shardIds, cursor, count, nameServer, scheduler)
  }
}

class MetadataDiff(shardIds: Seq[ShardId], cursor: MetadataRepair.RepairCursor, count: Int,
    nameServer: NameServer[Shard], scheduler: PrioritizingJobScheduler[JsonJob])
  extends MetadataRepair(shardIds, cursor, count, nameServer, scheduler) {

  private val log = Logger.get(getClass.getName)

  override def label = "MetadaDiff"

  override def scheduleMissing(list: (Shard, ListBuffer[Metadata], MetadataRepair.RepairCursor), tableId: Int, item: Metadata) = {
    log.info("DIFF [MISSING] -> table id:"+tableId+" shard:"+list._1.shardInfo.id+" metadata:"+item)
  }

  override def scheduleDifferent(list: (Shard, ListBuffer[Metadata], MetadataRepair.RepairCursor), tableId: Int, item: Metadata) = {
    log.info("DIFF [DIFFERENT] -> table id:"+tableId+" shard:"+list._1.shardInfo.id+" metadata:"+item)
  }

  override def scheduleNextRepair(lowestCursor: MetadataRepair.RepairCursor) = {
    lowestCursor match {
      case MetadataRepair.END => scheduler.put(Repair.PRIORITY, new Diff(shardIds, Repair.START, Repair.COUNT, nameServer, scheduler))
      case _ => scheduler.put(Repair.PRIORITY, new MetadataDiff(shardIds, lowestCursor, count, nameServer, scheduler))
    }
  }
}
