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
package shards

import com.twitter.gizzard.shards.RoutingNode
import com.twitter.util.Future
import com.twitter.util.Time

class ReadWriteShardAdapter(shard: RoutingNode[Shard]) extends Shard {
  def selectIncludingArchived(sourceId: Long, count: Int, cursor: Cursor)                     = shard.read.futureAny { _.selectIncludingArchived(sourceId, count, cursor) }
  def intersect(sourceId: Long, states: Seq[State], destinationIds: Seq[Long])                = shard.read.futureAny { _.intersect(sourceId, states, destinationIds) }
  def intersectEdges(sourceId: Long, states: Seq[State], destinationIds: Seq[Long])           = shard.read.futureAny { _.intersectEdges(sourceId, states, destinationIds) }
  def getMetadata(sourceId: Long)                                                             = shard.read.futureAny { _.getMetadata(sourceId) }
  def getMetadataForWrite(sourceId: Long)                                                     = shard.read.futureAny { _.getMetadataForWrite(sourceId) }
  def selectByDestinationId(sourceId: Long, states: Seq[State], count: Int, cursor: Cursor)   = shard.read.futureAny { _.selectByDestinationId(sourceId, states, count, cursor) }
  def selectByPosition(sourceId: Long, states: Seq[State], count: Int, cursor: Cursor)        = shard.read.futureAny { _.selectByPosition(sourceId, states, count, cursor) }
  def selectEdges(sourceId: Long, states: Seq[State], count: Int, cursor: Cursor)             = shard.read.futureAny { _.selectEdges(sourceId, states, count, cursor) }
  def selectAll(cursor: (Cursor, Cursor), count: Int)                                         = shard.read.futureAny { _.selectAll(cursor, count) }
  def selectAllMetadata(cursor: Cursor, count: Int)                                           = shard.read.futureAny { _.selectAllMetadata(cursor, count) }
  def get(sourceId: Long, destinationId: Long)                                                = shard.read.futureAny { _.get(sourceId, destinationId) }
  def count(sourceId: Long, states: Seq[State])                                               = shard.read.futureAny { _.count(sourceId, states) }
                                                                                             
  def bulkUnsafeInsertEdges(edges: Seq[Edge])                                                 = Future.join(shard.write.fmap { _.bulkUnsafeInsertEdges(edges) })
  def bulkUnsafeInsertMetadata(metadata: Seq[Metadata])                                       = Future.join(shard.write.fmap { _.bulkUnsafeInsertMetadata(metadata) })
                                                                                             
  def writeCopies(edges: Seq[Edge])                                                           = Future.join(shard.write.fmap { _.writeCopies(edges) })
  def writeMetadata(metadata: Metadata)                                                       = Future.join(shard.write.fmap { _.writeMetadata(metadata) })
  def writeMetadatas(metadata: Seq[Metadata])                                                 = Future.join(shard.write.fmap { _.writeMetadatas(metadata) })
  def updateMetadata(metadata: Metadata)                                                      = Future.join(shard.write.fmap { _.updateMetadata(metadata) })
  def remove(sourceId: Long, updatedAt: Time)                                                 = Future.join(shard.write.fmap { _.remove(sourceId, updatedAt) })
  def remove(sourceId: Long, destinationId: Long, position: Long, updatedAt: Time)            = Future.join(shard.write.fmap { _.remove(sourceId, destinationId, position, updatedAt) })
  def add(sourceId: Long, updatedAt: Time)                                                    = Future.join(shard.write.fmap { _.add(sourceId, updatedAt) })
  def add(sourceId: Long, destinationId: Long, position: Long, updatedAt: Time)               = Future.join(shard.write.fmap { _.add(sourceId, destinationId, position, updatedAt) })
  def negate(sourceId: Long, updatedAt: Time)                                                 = Future.join(shard.write.fmap { _.negate(sourceId, updatedAt) })
  def negate(sourceId: Long, destinationId: Long, position: Long, updatedAt: Time)            = Future.join(shard.write.fmap { _.negate(sourceId, destinationId, position, updatedAt) })
  def archive(sourceId: Long, destinationId: Long, position: Long, updatedAt: Time)           = Future.join(shard.write.fmap { _.archive(sourceId, destinationId, position, updatedAt) })
  def archive(sourceId: Long, updatedAt: Time)                                                = Future.join(shard.write.fmap { _.archive(sourceId, updatedAt) })
}
