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

import scala.collection.mutable
import com.twitter.gizzard.shards.RoutingNode
import com.twitter.util.Time
import com.twitter.util.TimeConversions._

class ReadWriteShardAdapter(shard: RoutingNode[Shard])
      extends Shard {
  def selectIncludingArchived(sourceId: Long, count: Int, cursor: Cursor)                            = shard.readOperation(_.selectIncludingArchived(sourceId, count, cursor))
  def intersect(sourceId: Long, states: Seq[State], destinationIds: Seq[Long])                       = shard.readOperation(_.intersect(sourceId, states, destinationIds))
  def intersectEdges(sourceId: Long, states: Seq[State], destinationIds: Seq[Long])                  = shard.readOperation(_.intersectEdges(sourceId, states, destinationIds))
  def getMetadata(sourceId: Long)                                                                    = shard.readOperation(_.getMetadata(sourceId))
  def getMetadataForWrite(sourceId: Long)                                                            = shard.readOperation(_.getMetadataForWrite(sourceId))
  def selectByDestinationId(sourceId: Long, states: Seq[State], count: Int, cursor: Cursor)          = shard.readOperation(_.selectByDestinationId(sourceId, states, count, cursor))
  def selectByPosition(sourceId: Long, states: Seq[State], count: Int, cursor: Cursor)               = shard.readOperation(_.selectByPosition(sourceId, states, count, cursor))
  def selectEdges(sourceId: Long, states: Seq[State], count: Int, cursor: Cursor)                    = shard.readOperation(_.selectEdges(sourceId, states, count, cursor))
  def selectAll(cursor: (Cursor, Cursor), count: Int)                                                = shard.readOperation(_.selectAll(cursor, count))
  def selectAllMetadata(cursor: Cursor, count: Int)                                                  = shard.readOperation(_.selectAllMetadata(cursor, count))
  def get(sourceId: Long, destinationId: Long)                                                       = shard.readOperation(_.get(sourceId, destinationId))
  def count(sourceId: Long, states: Seq[State])                                                      = shard.readOperation(_.count(sourceId, states))

  def bulkUnsafeInsertEdges(edges: Seq[Edge])                                                        = shard.writeOperation(_.bulkUnsafeInsertEdges(edges))
  def bulkUnsafeInsertMetadata(metadata: Seq[Metadata])                                              = shard.writeOperation(_.bulkUnsafeInsertMetadata(metadata))

  def writeCopies(edges: Seq[Edge])                                                                  = shard.writeOperation(_.writeCopies(edges))
  def writeMetadata(metadata: Metadata)                                                              = shard.writeOperation(_.writeMetadata(metadata))
  def writeMetadata(metadata: Seq[Metadata])                                                         = shard.writeOperation(_.writeMetadata(metadata))
  def updateMetadata(metadata: Metadata)                                                             = shard.writeOperation(_.updateMetadata(metadata))
  def remove(sourceId: Long, updatedAt: Time)                                                        = shard.writeOperation(_.remove(sourceId, updatedAt))
  def remove(sourceId: Long, destinationId: Long, position: Long, updatedAt: Time)                   = shard.writeOperation(_.remove(sourceId, destinationId, position, updatedAt))
  def add(sourceId: Long, updatedAt: Time)                                                           = shard.writeOperation(_.add(sourceId, updatedAt))
  def add(sourceId: Long, destinationId: Long, position: Long, updatedAt: Time)                      = shard.writeOperation(_.add(sourceId, destinationId, position, updatedAt))
  def negate(sourceId: Long, updatedAt: Time)                                                        = shard.writeOperation(_.negate(sourceId, updatedAt))
  def negate(sourceId: Long, destinationId: Long, position: Long, updatedAt: Time)                   = shard.writeOperation(_.negate(sourceId, destinationId, position, updatedAt))
  def archive(sourceId: Long, destinationId: Long, position: Long, updatedAt: Time)                  = shard.writeOperation(_.archive(sourceId, destinationId, position, updatedAt))
  def archive(sourceId: Long, updatedAt: Time)                                                       = shard.writeOperation(_.archive(sourceId, updatedAt))

  def getString = {
    shard.shardInfo.id.toString()
  }
}
