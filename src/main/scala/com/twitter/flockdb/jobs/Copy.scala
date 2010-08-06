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


object Copy {
  type Cursor = (results.Cursor, results.Cursor)

  val START = (results.Cursor.Start, results.Cursor.Start)
  val END = (results.Cursor.End, results.Cursor.End)
  val COUNT = 10000
}

object CopyFactory extends gizzard.jobs.CopyFactory[Shard] {
  def apply(sourceShardId: ShardId, destinationShardId: ShardId) = new MetadataCopy(sourceShardId, destinationShardId, MetadataCopy.START)
}

object CopyParser extends gizzard.jobs.CopyParser[Shard] {
  def apply(attributes: Map[String, Any]) = {
    val casted = attributes.asInstanceOf[Map[String, AnyVal]]
    new Copy(
      ShardId(casted("source_shard_hostname").toString, casted("source_shard_table_prefix").toString),
      ShardId(casted("destination_shard_hostname").toString, casted("destination_shard_table_prefix").toString),
      (results.Cursor(casted("cursor1").toInt), results.Cursor(casted("cursor2").toInt)),
      casted("count").toInt)
  }
}

class Copy(sourceShardId: ShardId, destinationShardId: ShardId, cursor: Copy.Cursor, count: Int) extends gizzard.jobs.Copy[Shard](sourceShardId, destinationShardId, count) {
  def this(sourceShardId: ShardId, destinationShardId: ShardId, cursor: Copy.Cursor) = this(sourceShardId, destinationShardId, cursor, Copy.COUNT)

  def copyPage(sourceShard: Shard, destinationShard: Shard, count: Int) = {
    val (items, newCursor) = sourceShard.selectAll(cursor, count)
    destinationShard.writeCopies(items)
    Stats.incr("edges-copy", items.size)
    if (newCursor == Copy.END) {
      None
    } else {
      Some(new Copy(sourceShardId, destinationShardId, newCursor, count))
    }
  }

  def serialize = Map("cursor1" -> cursor._1.position, "cursor2" -> cursor._2.position)
}

object MetadataCopy {
  type Cursor = results.Cursor
  val START = results.Cursor.Start
  val END = results.Cursor.End
}

object MetadataCopyParser extends gizzard.jobs.CopyParser[Shard] {
  def apply(attributes: Map[String, Any]) = {
    val casted = attributes.asInstanceOf[Map[String, AnyVal]]
    new MetadataCopy(
      ShardId(casted("source_shard_hostname").toString, casted("source_shard_table_prefix").toString),
      ShardId(casted("destination_shard_hostname").toString, casted("destination_shard_table_prefix").toString),
      results.Cursor(casted("cursor").toInt),
      casted("count").toInt)
  }
}

class MetadataCopy(sourceShardId: ShardId, destinationShardId: ShardId, cursor: MetadataCopy.Cursor,
                   count: Int)
      extends gizzard.jobs.Copy[Shard](sourceShardId, destinationShardId, count) {
  def this(sourceShardId: ShardId, destinationShardId: ShardId, cursor: MetadataCopy.Cursor) =
    this(sourceShardId, destinationShardId, cursor, Copy.COUNT)

  def copyPage(sourceShard: Shard, destinationShard: Shard, count: Int) = {
    val (items, newCursor) = sourceShard.selectAllMetadata(cursor, count)
    items.foreach { destinationShard.writeMetadata(_) }
    Stats.incr("edges-copy", items.size)
    if (newCursor == MetadataCopy.END)
      Some(new Copy(sourceShardId, destinationShardId, Copy.START))
    else
      Some(new MetadataCopy(sourceShardId, destinationShardId, newCursor, count))
  }

  def serialize = Map("cursor" -> cursor.position)
}
