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

object Repair {
  type RepairCursor = (Cursor, Cursor)
  val START = (Cursor.Start, Cursor.Start)
  val END = (Cursor.End, Cursor.End)
  val COUNT = 10000
  val PRIORITY = Priority.Low.id
}

class RepairFactory(nameServer: NameServer[Shard], scheduler: PrioritizingJobScheduler[JsonJob])
      extends RepairJobFactory[Shard, Metadata] {
  def apply(sourceShardId: ShardId, destShardId: ShardId) = {
    new MetadataRepair(sourceShardId, destShardId, MetadataRepair.START, MetadataRepair.START, MetadataRepair.COUNT, nameServer, scheduler)
  }
}

class RepairParser(nameServer: NameServer[Shard], scheduler: PrioritizingJobScheduler[JsonJob])
      extends RepairJobParser[Shard, Edge] {
  def deserialize(attributes: Map[String, Any], sourceId: ShardId, destinationId: ShardId, count: Int) = {
    val srcCursor = (Cursor(attributes("src_cursor1").asInstanceOf[AnyVal].toLong),
                    Cursor(attributes("src_cursor2").asInstanceOf[AnyVal].toLong))
    val destCursor = (Cursor(attributes("dest_cursor1").asInstanceOf[AnyVal].toLong),
                    Cursor(attributes("dest_cursor2").asInstanceOf[AnyVal].toLong))
    new Repair(sourceId, destinationId, srcCursor, destCursor, count, nameServer, scheduler)
  }
}

class Repair(sourceShardId: ShardId, destinationShardId: ShardId, srcCursor: Repair.RepairCursor,
           destCursor: Repair.RepairCursor, count: Int, nameServer: NameServer[Shard], 
           scheduler: PrioritizingJobScheduler[JsonJob])
      extends RepairJob[Shard, Edge](sourceShardId, destinationShardId, count, nameServer, scheduler, Repair.PRIORITY) {

  private val log = Logger.get(getClass.getName)

  def generateCursor(edge: Edge) = {
    (Cursor(edge.sourceId), Cursor(edge.destinationId)) 
  }

  def repair(sourceShard: Shard, destinationShard: Shard) = {
    val (srcSeq,  newSrcCursor) = sourceShard.selectAll(srcCursor, count)
    val (destSeq, newDestCursor) = destinationShard.selectAll(destCursor, count)
    println("getting forwarding!")
    val sourceTableId = nameServer.getRootForwardings(sourceShard.shardInfo.id)(0).tableId
    println("getting forwarding!")
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
      extends RepairJobParser[Shard, Metadata] {
  def deserialize(attributes: Map[String, Any], sourceId: ShardId, destinationId: ShardId, count: Int) = {
    val srcCursor  = Cursor(attributes("src_cursor").asInstanceOf[AnyVal].toLong)
    val destCursor = Cursor(attributes("dest_cursor").asInstanceOf[AnyVal].toLong)
    new MetadataRepair(sourceId, destinationId, srcCursor, destCursor, count, nameServer, scheduler)
  }
}

class MetadataRepair(sourceShardId: ShardId, destinationShardId: ShardId, srcCursor: MetadataRepair.RepairCursor,
     destCursor: MetadataRepair.RepairCursor, count: Int, nameServer: NameServer[Shard], scheduler: PrioritizingJobScheduler[JsonJob])
      extends RepairJob[Shard, Metadata](sourceShardId, destinationShardId, count, nameServer, scheduler, MetadataRepair.PRIORITY) {

  private val log = Logger.get(getClass.getName)

  def scheduleNextRepair(srcEdge: Option[Metadata], newSrcCursor: MetadataRepair.RepairCursor, destEdge: Option[Metadata], newDestCursor: MetadataRepair.RepairCursor) = {
    println("scheduleNextRepair")
    scheduler.put(MetadataRepair.PRIORITY, (newSrcCursor, newDestCursor) match {
      case (MetadataRepair.END, MetadataRepair.END) =>
        println("making a repair")
        new Repair(sourceShardId, destinationShardId, Repair.START, Repair.START, Repair.COUNT, nameServer, scheduler)
      case (_, _) => 
        incrGauge
        println("doing metadata work")
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
    println("repair: source:"+sourceShard+" dest:"+destinationShard)
    val (srcSeq,  newSrcCursor) = sourceShard.selectAllMetadata(srcCursor, count)
    val (destSeq, newDestCursor) = destinationShard.selectAllMetadata(destCursor, count)
    println("getting forwarding!")
    val sourceTableId = nameServer.getRootForwardings(sourceShard.shardInfo.id)(0).tableId
    println("getting forwarding!")
    val destinationTableId = nameServer.getRootForwardings(destinationShard.shardInfo.id)(0).tableId
    if (sourceTableId != destinationTableId) {
      println("no match!")
      throw new RuntimeException(sourceShard+" tableId did not match "+destinationShard);
    } else {
      println("resolving!")
      val (srcMetadata, destMetadata) = resolve(sourceTableId, srcSeq, newSrcCursor == MetadataRepair.END, destSeq, newDestCursor == MetadataRepair.END)
      scheduleNextRepair(srcMetadata, newSrcCursor, destMetadata, newDestCursor)
    }
  }

  def serialize = Map("src_cursor" -> srcCursor.position, "dest_cursor" -> destCursor.position)
}
