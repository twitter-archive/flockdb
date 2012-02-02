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
import com.twitter.gizzard.shards.{ShardId, RoutingNode}
import com.twitter.gizzard.nameserver.NameServer
import com.twitter.gizzard.Stats
import com.twitter.util.TimeConversions._
import conversions.Numeric._
import shards.{Shard, ReadWriteShardAdapter}
import scala.collection.mutable


object Copy {
  type CopyCursor = (Cursor, Cursor)

  val START = (Cursor.Start, Cursor.Start)
  val END = (Cursor.End, Cursor.End)
  val COUNT = 10000
}

class CopyFactory(nameServer: NameServer, scheduler: JobScheduler)
      extends CopyJobFactory[Shard] {
  def apply(shardIds: Seq[ShardId]) =
    new MetadataCopy(shardIds, MetadataCopy.START, Copy.COUNT,
                     nameServer, scheduler)
}

class CopyParser(nameServer: NameServer, scheduler: JobScheduler)
      extends CopyJobParser[Shard] {
  def deserialize(attributes: Map[String, Any], shardIds: Seq[ShardId], count: Int) = {
    val cursor = (Cursor(attributes("cursor1").asInstanceOf[AnyVal].toLong),
                  Cursor(attributes("cursor2").asInstanceOf[AnyVal].toLong))
    new Copy(shardIds, cursor, count, nameServer, scheduler)
  }


}

case class CopyState(
  var pos: Int,
  items: Seq[Edge],
  cursor: Copy.CopyCursor,
  total: Int,
  val diffs: mutable.ArrayBuffer[Edge]
)

/** Given a seq of shards, compares them and copies the most up to date data between them */
class Copy(shardIds: Seq[ShardId], var cursor: Copy.CopyCursor,
           count: Int, nameServer: NameServer, scheduler: JobScheduler)
      extends CopyJob[Shard](shardIds, count, nameServer, scheduler) {

  def copyPage(nodes: Seq[RoutingNode[Shard]], count: Int) = {
    val shards = nodes map { new ReadWriteShardAdapter(_) }

    var cursors = Seq(cursor)

    while (!cursors.isEmpty) {
      cursor = cursors.min
      val shardStates = shards map { shard =>
        val (edges, nextCursor) = shard.selectAll(cursor, count)()
        shard -> CopyState(0, edges, nextCursor, edges.size, mutable.ArrayBuffer[Edge]())
      } toMap

      /*
       * Loop through the edges we got and add diffs to each. Stop when we either run out of edges to process or we get through
       * one shard's batch of edges but haven't reached its END. Stopping in the latter case saves cycles that we'll
       * repeat on the next iteration anyway and potentially saves us from recording useless diffs.
      */
      while (shardStates.find { case (shard, state) => state.pos < state.total }.isDefined && shardStates.find { case (shard, state) => state.pos >= state.total && state.cursor != Copy.END}.isEmpty ) {
        val edges = shardStates.map { case (shard, state) =>
          val edge = if (state.pos < state.total) state.items(state.pos) else Edge.Max
          (shard, edge)
        }

        val (minShard, minEdge) = edges.foldLeft(edges.head) { case (min, pair) =>
          val minEdge = min._2
          val pairEdge = pair._2

          if (pairEdge.similar(minEdge) < 0) pair else min
        }

        val sameEdges = edges.filter { case (shard, edge) => edge.similar(minEdge) == 0 }

        val (bestShard, bestEdge) = sameEdges.foldLeft((minShard, minEdge)) { case (newest, pair) =>
          if (pair._2.updatedAt > newest._2.updatedAt) pair else newest
        }
        edges.foreach { case (shard, edge) =>
          if (bestEdge.similar(edge) < 0) {
            shardStates(shard).diffs += bestEdge
          } else if (bestEdge.similar(edge) == 0) {
            if (bestEdge.updatedAt > edge.updatedAt) {
              shardStates(shard).diffs += bestEdge
            }
            shardStates(shard).pos += 1
          }
        }
      }

      shardStates.foreach { case (shard, state) =>
        shard.writeCopies(state.diffs)()
        Stats.incr("edges-copy", state.diffs.size)
        state.diffs.clear
      }

      cursors = shardStates.toSeq.map { case (shard, state) => state.cursor}.filterNot{ _ == Copy.END }
    }

    None
  }

  def serialize = Map("cursor1" -> cursor._1.position, "cursor2" -> cursor._2.position)
}

object MetadataCopy {
  type CopyCursor = Cursor
  val START = Cursor.Start
  val END = Cursor.End
}

class MetadataCopyParser(nameServer: NameServer, scheduler: JobScheduler)
      extends CopyJobParser[Shard] {
  def deserialize(attributes: Map[String, Any], shardIds: Seq[ShardId], count: Int) = {
    val cursor = Cursor(attributes("cursor").asInstanceOf[AnyVal].toLong)
    new MetadataCopy(shardIds, cursor, count, nameServer, scheduler)
  }
}

case class MetadataCopyState(
  var pos: Int,
  items: Seq[Metadata],
  cursor: MetadataCopy.CopyCursor,
  total: Int,
  val diffs: mutable.ArrayBuffer[Metadata]
)

class MetadataCopy(shardIds: Seq[ShardId], var cursor: MetadataCopy.CopyCursor,
                   count: Int, nameServer: NameServer, scheduler: JobScheduler)
      extends CopyJob[Shard](shardIds, count, nameServer, scheduler) {

  def copyPage(nodes: Seq[RoutingNode[Shard]], count: Int) = {
    val shards = nodes.map { new ReadWriteShardAdapter(_) }

    var cursors = Seq(cursor)

    while(!cursors.isEmpty) {
      cursor = cursors.min

      val shardStates = Map(shards.map { shard =>
        val (items, nextCursor) = shard.selectAllMetadata(cursor, count)()
        (shard, MetadataCopyState(0, items, nextCursor, items.size, mutable.ArrayBuffer[Metadata]()))
      }: _*)

      while (shardStates.find{case (shard, state) => state.pos < state.total}.isDefined && shardStates.find{case (shard, state) => state.pos >= state.total && state.cursor != MetadataCopy.END}.isEmpty) {
        val items = shardStates.map { case (shard, state) =>
          val item = if (state.pos < state.total) state.items(state.pos) else Metadata.Max
          (shard, item)
        }

        val (minShard, minItem) = items.foldLeft(items.head) { case (min, pair) =>
          val minItem = min._2
          val pairItem = pair._2

          if (pairItem.similar(minItem) < 0) pair else min
        }

        val sameItems = items.filter { case (shard, item) => item.similar(minItem) == 0 }

        val (bestShard, bestItem) = sameItems.foldLeft((minShard, minItem)) { case (newest, pair) =>
          if (pair._2.updatedAt > newest._2.updatedAt) pair else newest }

        items.foreach { case (shard, item) =>
          if (bestItem.similar(item) < 0) {
            shardStates(shard).diffs += bestItem
          } else if (bestItem.similar(item) == 0) {
            if (bestItem.updatedAt > item.updatedAt) {
              shardStates(shard).diffs += bestItem
            }
            shardStates(shard).pos += 1
          }
        }
      }

      shardStates.foreach { case (shard, state) =>
        shard.writeMetadatas(state.diffs)()
        Stats.incr("edges-copy", state.diffs.size)
        state.diffs.clear
      }

      cursors = shardStates.toSeq.map { case (shard, state) => state.cursor }.filterNot{ _ == MetadataCopy.END }
    }

    Some(new Copy(shardIds, Copy.START, Copy.COUNT, nameServer, scheduler))

  }

  def serialize = Map("cursor" -> cursor.position)
}
