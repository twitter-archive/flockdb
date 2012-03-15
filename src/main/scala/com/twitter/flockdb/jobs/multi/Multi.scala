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

package com.twitter.flockdb.jobs.multi

import com.twitter.gizzard.scheduler._
import com.twitter.gizzard.shards.ShardBlackHoleException
import com.twitter.util.Time
import com.twitter.util.TimeConversions._
import com.twitter.flockdb.{State, ForwardingManager, Cursor, Priority, Direction}
import com.twitter.flockdb.conversions.Numeric._
import com.twitter.flockdb.shards.Shard
import com.twitter.flockdb.jobs.single.Single

// TODO: Make this async.
class MultiJobParser(
  forwardingManager: ForwardingManager,
  scheduler: PrioritizingJobScheduler,
  aggregateJobPageSize: Int)
extends JsonJobParser {

  def apply(attributes: Map[String, Any]): JsonJob = {
    val casted = attributes.asInstanceOf[Map[String, AnyVal]]

    new Multi(
      casted("source_id").toLong,
      casted("graph_id").toInt,
      Direction(casted("direction").toInt),
      State(casted("state").toInt),
      Time.fromSeconds(casted("updated_at").toInt),
      Priority(casted.get("priority").map(_.toInt).getOrElse(Priority.Low.id)),
      aggregateJobPageSize,
      casted.get("cursor").map( c => Cursor(c.toLong)).getOrElse(Cursor.Start),
      forwardingManager,
      scheduler
    )
  }
}

class Multi(
  sourceId: Long,
  graphId: Int,
  direction: Direction,
  preferredState: State,
  updatedAt: Time,
  priority: Priority.Value,
  aggregateJobPageSize: Int,
  var cursor: Cursor,
  forwardingManager: ForwardingManager,
  scheduler: PrioritizingJobScheduler)
extends JsonJob {

  def this(
    sourceId: Long,
    graphId: Int,
    direction: Direction,
    preferredState: State,
    updatedAt: Time,
    priority: Priority.Value,
    aggregateJobPageSize: Int,
    forwardingManager: ForwardingManager,
    scheduler: PrioritizingJobScheduler
  ) = {
    this(
      sourceId,
      graphId,
      direction,
      preferredState,
      updatedAt,
      priority,
      aggregateJobPageSize,
      Cursor.Start,
      forwardingManager,
      scheduler
    )
  }

  def toMap = Map(
    "source_id" -> sourceId,
    "updated_at" -> updatedAt.inSeconds,
    "graph_id" -> graphId,
    "direction" -> direction.id,
    "priority" -> priority.id,
    "state" -> preferredState.id,
    "cursor" -> cursor.position
  )

  def apply() {
    val forwardShard = forwardingManager.find(sourceId, graphId, direction)

    if (cursor == Cursor.Start) try {
      updateMetadata(forwardShard, preferredState)
    } catch {
      case e: ShardBlackHoleException => return
    }

    while (cursor != Cursor.End) {
      val resultWindow = forwardShard.selectIncludingArchived(sourceId, aggregateJobPageSize, cursor)()

      val chunkOfTasks = resultWindow.map { destinationId =>
        val (a, b) = if (direction == Direction.Backward) (destinationId, sourceId) else (sourceId, destinationId)
        singleEdgeJob(a, graphId, b, preferredState)
      }

      scheduler.put(priority.id, new JsonNestedJob(chunkOfTasks))

      // "commit" the current iteration by saving the next cursor.
      // if the job blows up in the next round, it will be re-serialized
      // with this cursor.
      cursor = resultWindow.nextCursor
    }
  }

  // XXX: since this job gets immediately serialized, pass null for forwardingManager and uuidGenerator.
  protected def singleEdgeJob(sourceId: Long, graphId: Int, destinationId: Long, state: State) = {
    new Single(sourceId, graphId, destinationId, updatedAt.inMillis, state, updatedAt, null, null)
  }

  protected def updateMetadata(shard: Shard, state: State) = state match {
    case State.Normal   => shard.add(sourceId, updatedAt)()
    case State.Removed  => shard.remove(sourceId, updatedAt)()
    case State.Archived => shard.archive(sourceId, updatedAt)()
    case State.Negative => shard.negate(sourceId, updatedAt)()
  }

  override def equals(o: Any) = o match {
    case o: Multi => this.toMap == o.toMap
    case _        => false
  }
}
