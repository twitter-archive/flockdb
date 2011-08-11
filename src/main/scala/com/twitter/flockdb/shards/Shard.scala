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
import com.twitter.gizzard.shards
import com.twitter.util.Time
import com.twitter.util.TimeConversions._
import com.twitter.gizzard.scheduler._

trait Shard {
  @throws(classOf[shards.ShardException]) def get(sourceId: Long, destinationId: Long): Option[Edge]
  @throws(classOf[shards.ShardException]) def getMetadata(sourceId: Long): Option[Metadata]
  @throws(classOf[shards.ShardException]) def getMetadataForWrite(sourceId: Long): Option[Metadata]

  @throws(classOf[shards.ShardException]) def getMetadatas(sourceId: Long): Seq[Either[Throwable,Option[Metadata]]] = {
    Seq(try { Right(getMetadataForWrite(sourceId)) } catch { case e => Left(e) })
  }

  @throws(classOf[shards.ShardException]) def optimistically(sourceId: Long)(f: State => Unit)

  @throws(classOf[shards.ShardException]) def count(sourceId: Long, states: Seq[State]): Int

  @throws(classOf[shards.ShardException]) def selectAll(cursor: (Cursor, Cursor), count: Int): (Seq[Edge], (Cursor, Cursor))
  @throws(classOf[shards.ShardException]) def selectAllMetadata(cursor: Cursor, count: Int): (Seq[Metadata], Cursor)
  @throws(classOf[shards.ShardException]) def selectIncludingArchived(sourceId: Long, count: Int, cursor: Cursor): ResultWindow[Long]
  @throws(classOf[shards.ShardException]) def selectByDestinationId(sourceId: Long, states: Seq[State], count: Int, cursor: Cursor): ResultWindow[Long]
  @throws(classOf[shards.ShardException]) def selectByPosition(sourceId: Long, states: Seq[State], count: Int, cursor: Cursor): ResultWindow[Long]
  @throws(classOf[shards.ShardException]) def selectEdges(sourceId: Long, states: Seq[State], count: Int, cursor: Cursor): ResultWindow[Edge]

  @throws(classOf[shards.ShardException]) def writeCopies(edge: Seq[Edge])
  @throws(classOf[shards.ShardException]) def updateMetadata(metadata: Metadata)
  @throws(classOf[shards.ShardException]) def writeMetadata(metadata: Metadata)
  @throws(classOf[shards.ShardException]) def writeMetadata(metadata: Seq[Metadata])

  @throws(classOf[shards.ShardException]) def bulkUnsafeInsertEdges(edge: Seq[Edge])
  @throws(classOf[shards.ShardException]) def bulkUnsafeInsertMetadata(edge: Seq[Metadata])

  @throws(classOf[shards.ShardException]) def archive(sourceId: Long, destinationId: Long, position: Long, updatedAt: Time)
  @throws(classOf[shards.ShardException]) def archive(sourceId: Long, updatedAt: Time)

  @throws(classOf[shards.ShardException]) def remove(sourceId: Long, destinationId: Long, position: Long, updatedAt: Time)
  @throws(classOf[shards.ShardException]) def remove(sourceId: Long, updatedAt: Time)

  @throws(classOf[shards.ShardException]) def add(sourceId: Long, destinationId: Long, position: Long, updatedAt: Time)
  @throws(classOf[shards.ShardException]) def add(sourceId: Long, updatedAt: Time)

  @throws(classOf[shards.ShardException]) def negate(sourceId: Long, destinationId: Long, position: Long, updatedAt: Time)
  @throws(classOf[shards.ShardException]) def negate(sourceId: Long, updatedAt: Time)

  @throws(classOf[shards.ShardException]) def intersect(sourceId: Long, states: Seq[State], destinationIds: Seq[Long]): Seq[Long]
  @throws(classOf[shards.ShardException]) def intersectEdges(sourceId: Long, states: Seq[State], destinationIds: Seq[Long]): Seq[Edge]
}
