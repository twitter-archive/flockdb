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

import java.util.TreeMap
import collection.mutable.ListBuffer
import com.twitter.gizzard.scheduler._
import com.twitter.gizzard.shards.ShardId
import com.twitter.gizzard.nameserver.NameServer
import com.twitter.ostrich.Stats
import com.twitter.util.TimeConversions._
import conversions.Numeric._
import shards.Shard


object Split {
  type SplitCursor = (Cursor, Cursor)

  val START = (Cursor.Start, Cursor.Start)
  val END = (Cursor.End, Cursor.End)
  val COUNT = 10000
}

class SplitFactory(nameServer: NameServer[Shard], scheduler: JobScheduler[JsonJob])
      extends CopyJobFactory[Shard] {
  def apply(sourceShardId: ShardId, destinations: List[CopyDestination]) =
    new MetadataSplit(sourceShardId, destinations, MetadataSplit.START, Split.COUNT,
                     nameServer, scheduler)
}

class SplitParser(nameServer: NameServer[Shard], scheduler: JobScheduler[JsonJob])
      extends CopyJobParser[Shard] {
  def deserialize(attributes: Map[String, Any], sourceId: ShardId, destinations: List[CopyDestination], count: Int) = {
    val cursor = (Cursor(attributes("cursor1").asInstanceOf[AnyVal].toLong),
                  Cursor(attributes("cursor2").asInstanceOf[AnyVal].toLong))
    new Split(sourceId, destinations, cursor, count, nameServer, scheduler)
  }
}

class Split(sourceShardId: ShardId, destinations: List[CopyDestination], cursor: Split.SplitCursor,
           count: Int, nameServer: NameServer[Shard], scheduler: JobScheduler[JsonJob])
      extends CopyJob[Shard](sourceShardId, destinations, count, nameServer, scheduler) {
  def copyPage(sourceShard: Shard, destinationShards: List[CopyDestinationShard[Shard]], count: Int) = {
    val (items, newCursor) = sourceShard.selectAll(cursor, count)

    val byBaseIds = new TreeMap[Long, (Shard, ListBuffer[Edge])]()
    destinationShards.foreach { d =>
      byBaseIds.put(d.baseId.getOrElse(0), ((d.shard, new ListBuffer[Edge])))
    }

    println("WHAT! " + byBaseIds)

    items.foreach { item =>
      byBaseIds.floorEntry(nameServer.mappingFunction(item.sourceId)).getValue._2 += item
    }

    val iter = byBaseIds.values.iterator
    while(iter.hasNext) {
      val (dest, list) = iter.next
      dest.writeCopies(list.toList)
    }

    Stats.incr("edges-split", items.size)
    if (newCursor == Split.END) {
      None
    } else {
      Some(new Split(sourceShardId, destinations, newCursor, count, nameServer, scheduler))
    }
  }

  def serialize = Map("cursor1" -> cursor._1.position, "cursor2" -> cursor._2.position)
}

object MetadataSplit {
  type SplitCursor = Cursor
  val START = Cursor.Start
  val END = Cursor.End
}

class MetadataSplitParser(nameServer: NameServer[Shard], scheduler: JobScheduler[JsonJob])
      extends CopyJobParser[Shard] {
  def deserialize(attributes: Map[String, Any], sourceId: ShardId, destinations: List[CopyDestination], count: Int) = {
    val cursor = Cursor(attributes("cursor").asInstanceOf[AnyVal].toLong)
    new MetadataSplit(sourceId, destinations, cursor, count, nameServer, scheduler)
  }
}

class MetadataSplit(sourceShardId: ShardId, destinations: List[CopyDestination], cursor: MetadataSplit.SplitCursor,
                   count: Int, nameServer: NameServer[Shard], scheduler: JobScheduler[JsonJob])
      extends CopyJob[Shard](sourceShardId, destinations, count, nameServer, scheduler) {
  def copyPage(sourceShard: Shard, destinationShards: List[CopyDestinationShard[Shard]], count: Int) = {
    val (items, newCursor) = sourceShard.selectAllMetadata(cursor, count)

    val byBaseIds = new TreeMap[Long, Shard]()
    destinationShards.foreach { d => byBaseIds.put(d.baseId.getOrElse(0), d.shard) }

    items.foreach { item =>
      val shard = byBaseIds.floorEntry(nameServer.mappingFunction(item.sourceId)).getValue
      shard.writeMetadata(item)
    }

    Stats.incr("edges-split", items.size)
    if (newCursor == MetadataSplit.END)
      Some(new Split(sourceShardId, destinations, Split.START, Split.COUNT, nameServer, scheduler))
    else
      Some(new MetadataSplit(sourceShardId, destinations, newCursor, count, nameServer, scheduler))
  }

  def serialize = Map("cursor" -> cursor.position)
}