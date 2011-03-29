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
import com.twitter.ostrich.stats.Stats
import com.twitter.util.TimeConversions._
import conversions.Numeric._
import shards.Shard


object Copy {
  type CopyCursor = (Cursor, Cursor)

  val START = (Cursor.Start, Cursor.Start)
  val END = (Cursor.End, Cursor.End)
  val COUNT = 10000
}

class CopyFactory(nameServer: NameServer[Shard], scheduler: JobScheduler)
      extends CopyJobFactory[Shard] {
  def apply(sourceShardId: ShardId, destinationShardId: ShardId) =
    new MetadataCopy(sourceShardId, destinationShardId, MetadataCopy.START, Copy.COUNT,
                     nameServer, scheduler)
}

class CopyParser(nameServer: NameServer[Shard], scheduler: JobScheduler)
      extends CopyJobParser[Shard] {
  def deserialize(attributes: Map[String, Any], sourceId: ShardId, destinationId: ShardId, count: Int) = {
    val cursor = (Cursor(attributes("cursor1").asInstanceOf[AnyVal].toLong),
                  Cursor(attributes("cursor2").asInstanceOf[AnyVal].toLong))
    new Copy(sourceId, destinationId, cursor, count, nameServer, scheduler)
  }
}

class Copy(sourceShardId: ShardId, destinationShardId: ShardId, cursor: Copy.CopyCursor,
           count: Int, nameServer: NameServer[Shard], scheduler: JobScheduler)
      extends CopyJob[Shard](sourceShardId, destinationShardId, count, nameServer, scheduler) {
  def copyPage(sourceShard: Shard, destinationShard: Shard, count: Int) = {
    val (items, newCursor) = sourceShard.selectAll(cursor, count)
    destinationShard.writeCopies(items)
    Stats.incr("edges-copy", items.size)
    if (newCursor == Copy.END) {
      None
    } else {
      Some(new Copy(sourceShardId, destinationShardId, newCursor, count, nameServer, scheduler))
    }
  }

  def serialize = Map("cursor1" -> cursor._1.position, "cursor2" -> cursor._2.position)
}

object MetadataCopy {
  type CopyCursor = Cursor
  val START = Cursor.Start
  val END = Cursor.End
}

class MetadataCopyParser(nameServer: NameServer[Shard], scheduler: JobScheduler)
      extends CopyJobParser[Shard] {
  def deserialize(attributes: Map[String, Any], sourceId: ShardId, destinationId: ShardId, count: Int) = {
    val cursor = Cursor(attributes("cursor").asInstanceOf[AnyVal].toLong)
    new MetadataCopy(sourceId, destinationId, cursor, count, nameServer, scheduler)
  }
}

class MetadataCopy(sourceShardId: ShardId, destinationShardId: ShardId, cursor: MetadataCopy.CopyCursor,
                   count: Int, nameServer: NameServer[Shard], scheduler: JobScheduler)
      extends CopyJob[Shard](sourceShardId, destinationShardId, count, nameServer, scheduler) {
  def copyPage(sourceShard: Shard, destinationShard: Shard, count: Int) = {
    val (items, newCursor) = sourceShard.selectAllMetadata(cursor, count)
    destinationShard.writeMetadata(items)
    Stats.incr("edges-copy", items.size)
    if (newCursor == MetadataCopy.END)
      Some(new Copy(sourceShardId, destinationShardId, Copy.START, Copy.COUNT, nameServer, scheduler))
    else
      Some(new MetadataCopy(sourceShardId, destinationShardId, newCursor, count, nameServer, scheduler))
  }

  def serialize = Map("cursor" -> cursor.position)
}
