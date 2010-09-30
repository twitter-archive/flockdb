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

import com.twitter.gizzard.jobs.BoundJobParser
import com.twitter.gizzard.scheduler.JobScheduler
import com.twitter.gizzard.shards.ShardId
import com.twitter.gizzard.nameserver.NameServer
import com.twitter.results
import com.twitter.ostrich.Stats
import com.twitter.xrayspecs.TimeConversions._
import net.lag.logging.Logger
import shards.Shard


object UnsafeCopy {
  type Cursor = (results.Cursor, results.Cursor)

  val START = (results.Cursor.Start, results.Cursor.Start)
  val END = (results.Cursor.End, results.Cursor.End)
  val COUNT = 10000
}

object UnsafeCopyFactory extends gizzard.jobs.CopyFactory[Shard] {
  def apply(sourceShardId: ShardId, destinationShardId: ShardId) = new MetadataUnsafeCopy(sourceShardId, destinationShardId, MetadataUnsafeCopy.START)
}

object UnsafeCopyParser extends gizzard.jobs.CopyParser[Shard] {
  def apply(attributes: Map[String, Any]) = {
    val casted = attributes.asInstanceOf[Map[String, AnyVal]]
    new UnsafeCopy(
      ShardId(casted("source_shard_hostname").toString, casted("source_shard_table_prefix").toString),
      ShardId(casted("destination_shard_hostname").toString, casted("destination_shard_table_prefix").toString),
      (results.Cursor(casted("cursor1").toInt), results.Cursor(casted("cursor2").toInt)),
      casted("count").toInt)
  }
}

class UnsafeCopy(sourceShardId: ShardId, destinationShardId: ShardId, cursor: UnsafeCopy.Cursor, count: Int) extends gizzard.jobs.Copy[Shard](sourceShardId, destinationShardId, count) {
  def this(sourceShardId: ShardId, destinationShardId: ShardId, cursor: UnsafeCopy.Cursor) = this(sourceShardId, destinationShardId, cursor, UnsafeCopy.COUNT)

  def copyPage(sourceShard: Shard, destinationShard: Shard, count: Int) = {
    val (items, newCursor) = sourceShard.selectAll(cursor, count)
    destinationShard.bulkUnsafeInsertEdges(items)
    Stats.incr("edges-copy", items.size)
    if (newCursor == UnsafeCopy.END) {
      None
    } else {
      Some(new UnsafeCopy(sourceShardId, destinationShardId, newCursor, count))
    }
  }

  def serialize = Map("cursor1" -> cursor._1.position, "cursor2" -> cursor._2.position)
}

object MetadataUnsafeCopy {
  type Cursor = results.Cursor
  val START = results.Cursor.Start
  val END = results.Cursor.End
}

object MetadataUnsafeCopyParser extends gizzard.jobs.CopyParser[Shard] {
  def apply(attributes: Map[String, Any]) = {
    val casted = attributes.asInstanceOf[Map[String, AnyVal]]
    new MetadataUnsafeCopy(
      ShardId(casted("source_shard_hostname").toString, casted("source_shard_table_prefix").toString),
      ShardId(casted("destination_shard_hostname").toString, casted("destination_shard_table_prefix").toString),
      results.Cursor(casted("cursor").toInt),
      casted("count").toInt)
  }
}

class MetadataUnsafeCopy(sourceShardId: ShardId, destinationShardId: ShardId, cursor: MetadataUnsafeCopy.Cursor,
                   count: Int)
      extends gizzard.jobs.Copy[Shard](sourceShardId, destinationShardId, count) {
  def this(sourceShardId: ShardId, destinationShardId: ShardId, cursor: MetadataUnsafeCopy.Cursor) =
    this(sourceShardId, destinationShardId, cursor, UnsafeCopy.COUNT)

  def copyPage(sourceShard: Shard, destinationShard: Shard, count: Int) = {
    val (items, newCursor) = sourceShard.selectAllMetadata(cursor, count)
    destinationShard.bulkUnsafeInsertMetadata(items)
    Stats.incr("edges-copy", items.size)
    if (newCursor == MetadataUnsafeCopy.END)
      Some(new UnsafeCopy(sourceShardId, destinationShardId, UnsafeCopy.START))
    else
      Some(new MetadataUnsafeCopy(sourceShardId, destinationShardId, newCursor, count))
  }

  def serialize = Map("cursor" -> cursor.position)
}
