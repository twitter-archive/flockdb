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

package com.twitter.flockdb.shards

import scala.collection.mutable
import com.twitter.gizzard.shards
import com.twitter.util.Time
import com.twitter.util.TimeConversions._
import flockdb.jobs.multi._
import com.twitter.gizzard.scheduler._

case class Metadata(sourceId: Long, state: State, count: Int, updatedAt: Time) extends Repairable[Metadata] {
  def schedule(tableId: Int, forwardingManager: ForwardingManager, scheduler: PrioritizingJobScheduler[JsonJob]) = {
    val job = state match {
      case State.Normal => Unarchive
      case State.Removed => RemoveAll
      case State.Archived => Archive
      case State.Negative => Negate
    }

    scheduler.put(Priority.Medium.id, job(sourceId, tableId, if (tableId > 0) Direction.Forward else Direction.Backward, updatedAt, Priority.Medium, 500, forwardingManager, scheduler))
  }

  def similar(other: Metadata) = {
    sourceId.compare(other.sourceId)
    //if (sourceId < other.sourceId) -1
    //else if (sourceId > other.sourceId) 1
    //else 0
  }

  def compare(other: Metadata) = {
    updatedAt.compare(other.updatedAt) match {
      case x if x < 0 => -1
      case x if x > 0 => 1
      case _ => state.compare(other.state)
    }
  }
}

trait Shard extends shards.Shard {
  @throws(classOf[shards.ShardException]) def get(sourceId: Long, destinationId: Long): Option[Edge]
  @throws(classOf[shards.ShardException]) def getMetadata(sourceId: Long): Option[Metadata]
  @throws(classOf[shards.ShardException]) def withLock[A](sourceId: Long)(f: (Shard, Metadata) => A): A
  @throws(classOf[shards.ShardException]) def count(sourceId: Long, states: Seq[State]): Int
  @throws(classOf[shards.ShardException]) def counts(sourceIds: Seq[Long], results: mutable.Map[Long, Int])

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
