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

class Copy(sourceShardId: ShardId, destinationShardId: ShardId, cursor: Copy.Cursor, count: Int) extends gizzard.jobs.Copy[Shard](sourceShardId, destinationShardId, count) {
  def this(sourceShardId: ShardId, destinationShardId: ShardId, cursor: Copy.Cursor) = this(sourceShardId, destinationShardId, cursor, Copy.COUNT)
  def this(attributes: Map[String, AnyVal]) = {
    this(
      ShardId(attributes("source_shard_hostname").toString, attributes("source_shard_table_prefix").toString),
      ShardId(attributes("destination_shard_hostname").toString, attributes("destination_shard_table_prefix").toString),
      (results.Cursor(attributes("cursor1").toInt), results.Cursor(attributes("cursor2").toInt)),
      attributes("count").toInt)
  }
  
  // This is called multiple times, but we only want to call startEdgeCopy once
  override def apply(environment: (NameServer[Shard], JobScheduler)) {
    val (nameServer, scheduler) = environment
    if (cursor == Copy.START) {
      nameServer.findShardById(destinationShardId).startEdgeCopy()
    }
    super.apply(environment)
  }

  // This is called once, at end of shard copy
  override def finish(nameServer: NameServer[Shard], scheduler: JobScheduler) {
    nameServer.findShardById(destinationShardId).finishEdgeCopy()
    super.finish(nameServer, scheduler)
  }
  
  def copyPage(sourceShard: Shard, destinationShard: Shard, count: Int) = {
    if (destinationShard.needsEdgeCopyStart()) {
      // If a flapp goes down mid copy, we get a copyPage call without corresponding startEdgeCopy
      // Do any cleanup in startEdgeCopy
      Some(new Copy(sourceShardId, destinationShardId, Copy.START, count))
    } else {
      val (items, nextCursor) = sourceShard.selectAll(cursor, count)  
      destinationShard.writeCopies(items)
      Stats.incr("edges-copy", items.size)
      nextCursor match {
        case Copy.END => None
        case other => Some(new Copy(sourceShardId, destinationShardId, nextCursor, count))
      }
    }
  }

  def serialize = Map("cursor1" -> cursor._1.position, "cursor2" -> cursor._2.position)
}

object MetadataCopy {
  type Cursor = results.Cursor
  val START = results.Cursor.Start
  val END = results.Cursor.End
}

class MetadataCopy(sourceShardId: ShardId, destinationShardId: ShardId, cursor: MetadataCopy.Cursor,
                   count: Int)
      extends gizzard.jobs.Copy[Shard](sourceShardId, destinationShardId, count) {
  def this(sourceShardId: ShardId, destinationShardId: ShardId, cursor: MetadataCopy.Cursor) =
    this(sourceShardId, destinationShardId, cursor, Copy.COUNT)

  def this(attributes: Map[String, AnyVal]) = {
    this(
      ShardId(attributes("source_shard_hostname").toString, attributes("source_shard_table_prefix").toString),
      ShardId(attributes("destination_shard_hostname").toString, attributes("destination_shard_table_prefix").toString),
      results.Cursor(attributes("cursor").toInt),
      attributes("count").toInt)
  }
  
  // This is called multiple times, but we only want to call startMetadataCopy once
  override def apply(environment: (NameServer[Shard], JobScheduler)) {
    val (nameServer, scheduler) = environment
    if (cursor == MetadataCopy.START) {
      nameServer.findShardById(destinationShardId).startMetadataCopy()
    }
    super.apply(environment)
  }

  def copyPage(sourceShard: Shard, destinationShard: Shard, count: Int) = {
    if (destinationShard.needsMetadataCopyStart()) {
      // If a flapp goes down mid copy, we get a copyPage call without corresponding startMetadataCopy
      // Do any cleanup in startMetadataCopy
      Some(new MetadataCopy(sourceShardId, destinationShardId, MetadataCopy.START, count))
    } else {
      val (items, nextCursor) = sourceShard.selectAllMetadata(cursor, count)
      items.foreach { item => destinationShard.writeMetadata(item) }
      Stats.incr("metadata-copy", items.size)
      nextCursor match {
        case MetadataCopy.END => {
          destinationShard.finishMetadataCopy()
          Some(new Copy(sourceShardId, destinationShardId, Copy.START))
        }
        case other => Some(new MetadataCopy(sourceShardId, destinationShardId, nextCursor, count))
      }
    }
  }

  def serialize = Map("cursor" -> cursor.position)
}
