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
import shards.Shard


object UnsafeCopy {
  type CopyCursor = (Cursor, Cursor)

  val START = (Cursor.Start, Cursor.Start)
  val END = (Cursor.End, Cursor.End)
  val COUNT = 10000
}

class UnsafeCopyFactory(nameServer: NameServer[Shard], scheduler: JobScheduler[JsonJob])
      extends CopyJobFactory[Shard] {
  def apply(sourceShardId: ShardId, destinations: List[CopyDestination]) =
    new MetadataUnsafeCopy(sourceShardId, destinations, MetadataUnsafeCopy.START,
                           UnsafeCopy.COUNT, nameServer, scheduler)
}

class UnsafeCopyParser(nameServer: NameServer[Shard], scheduler: JobScheduler[JsonJob])
      extends CopyJobParser[Shard] {
  def deserialize(attributes: Map[String, Any], sourceId: ShardId, destinations: List[CopyDestination], count: Int) = {
    val cursor = (Cursor(attributes("cursor1").asInstanceOf[AnyVal].toInt),
                  Cursor(attributes("cursor2").asInstanceOf[AnyVal].toInt))
    new UnsafeCopy(sourceId, destinations, cursor, count, nameServer, scheduler)
  }
}

class UnsafeCopy(sourceShardId: ShardId, destinations: List[CopyDestination], cursor: UnsafeCopy.CopyCursor,
                 count: Int, nameServer: NameServer[Shard], scheduler: JobScheduler[JsonJob])
      extends CopyJob[Shard](sourceShardId, destinations, count, nameServer, scheduler) {
  def copyPage(sourceShard: Shard, destinationShards: List[CopyDestinationShard[Shard]], count: Int) = {
    val (items, newCursor) = sourceShard.selectAll(cursor, count)
    destinationShards.foreach(_.shard.bulkUnsafeInsertEdges(items))
    Stats.incr("edges-copy", items.size)
    if (newCursor == UnsafeCopy.END) {
      None
    } else {
      Some(new UnsafeCopy(sourceShardId, destinations, newCursor, count, nameServer, scheduler))
    }
  }

  def serialize = Map("cursor1" -> cursor._1.position, "cursor2" -> cursor._2.position)
}

object MetadataUnsafeCopy {
  type CopyCursor = Cursor
  val START = Cursor.Start
  val END = Cursor.End
}

class MetadataUnsafeCopyParser(nameServer: NameServer[Shard], scheduler: JobScheduler[JsonJob])
      extends CopyJobParser[Shard] {
  def deserialize(attributes: Map[String, Any], sourceId: ShardId, destinations: List[CopyDestination], count: Int) = {
    val cursor = Cursor(attributes("cursor").asInstanceOf[AnyVal].toInt)
    new MetadataUnsafeCopy(sourceId, destinations, cursor, count, nameServer, scheduler)
  }
}

class MetadataUnsafeCopy(sourceShardId: ShardId, destinations: List[CopyDestination],
                         cursor: MetadataUnsafeCopy.CopyCursor, count: Int,
                         nameServer: NameServer[Shard], scheduler: JobScheduler[JsonJob])
      extends CopyJob[Shard](sourceShardId, destinations, count, nameServer, scheduler) {
  def copyPage(sourceShard: Shard, destinationShards: List[CopyDestinationShard[Shard]], count: Int) = {
    val (items, newCursor) = sourceShard.selectAllMetadata(cursor, count)
    destinationShards.map(_.shard.bulkUnsafeInsertMetadata(items))
    Stats.incr("edges-copy", items.size)
    if (newCursor == MetadataUnsafeCopy.END)
      Some(new UnsafeCopy(sourceShardId, destinations, UnsafeCopy.START, UnsafeCopy.COUNT,
                          nameServer, scheduler))
    else
      Some(new MetadataUnsafeCopy(sourceShardId, destinations, newCursor, count, nameServer, scheduler))
  }

  def serialize = Map("cursor" -> cursor.position)
}
