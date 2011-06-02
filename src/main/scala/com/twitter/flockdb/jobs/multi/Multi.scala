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
package jobs.multi

import com.twitter.gizzard.scheduler._
import com.twitter.gizzard.shards.ShardBlackHoleException
import com.twitter.ostrich.Stats
import com.twitter.util.Time
import com.twitter.util.TimeConversions._
import net.lag.configgy.Configgy
import conversions.Numeric._
import shards.Shard
import State._

abstract class MultiJobParser extends JsonJobParser {
  def apply(attributes: Map[String, Any]): JsonJob = {
    val casted = attributes.asInstanceOf[Map[String, AnyVal]]
    createJob(
      casted("source_id").toLong,
      casted("graph_id").toInt,
      Direction(casted("direction").toInt),
      Time.fromSeconds(casted("updated_at").toInt),
      Priority(casted.get("priority").map(_.toInt).getOrElse(Priority.Low.id)))
  }

  protected def createJob(sourceId: Long, graphId: Int, direction: Direction, updatedAt: Time, priority: Priority.Value): Multi
}

class ArchiveParser(forwardingManager: ForwardingManager, scheduler: PrioritizingJobScheduler, aggregateJobPageSize: Int, uuidGenerator: UuidGenerator) extends MultiJobParser {
  protected def createJob(sourceId: Long, graphId: Int, direction: Direction, updatedAt: Time, priority: Priority.Value) = {
    new Archive(sourceId, graphId, direction, updatedAt, priority, aggregateJobPageSize, forwardingManager, scheduler, uuidGenerator)
  }
}

class UnarchiveParser(forwardingManager: ForwardingManager, scheduler: PrioritizingJobScheduler, aggregateJobPageSize: Int, uuidGenerator: UuidGenerator) extends MultiJobParser {
  protected def createJob(sourceId: Long, graphId: Int, direction: Direction, updatedAt: Time, priority: Priority.Value) = {
    new Unarchive(sourceId, graphId, direction, updatedAt, priority, aggregateJobPageSize, forwardingManager, scheduler, uuidGenerator)
  }
}

class RemoveAllParser(forwardingManager: ForwardingManager, scheduler: PrioritizingJobScheduler, aggregateJobPageSize: Int, uuidGenerator: UuidGenerator) extends MultiJobParser {
  protected def createJob(sourceId: Long, graphId: Int, direction: Direction, updatedAt: Time, priority: Priority.Value) = {
    new RemoveAll(sourceId, graphId, direction, updatedAt, priority, aggregateJobPageSize, forwardingManager, scheduler, uuidGenerator)
  }
}

class NegateParser(forwardingManager: ForwardingManager, scheduler: PrioritizingJobScheduler, aggregateJobPageSize: Int, uuidGenerator: UuidGenerator) extends MultiJobParser {
  protected def createJob(sourceId: Long, graphId: Int, direction: Direction, updatedAt: Time, priority: Priority.Value) = {
    new Negate(sourceId, graphId, direction, updatedAt, priority, aggregateJobPageSize, forwardingManager, scheduler, uuidGenerator)
  }
}

abstract class Multi(sourceId: Long, graphId: Int, direction: Direction, updatedAt: Time,
               priority: Priority.Value, aggregateJobPageSize: Int, forwardingManager: ForwardingManager,
               scheduler: PrioritizingJobScheduler, uuidGenerator: UuidGenerator)
         extends JsonJob {
  private val config = Configgy.config

  def toMap = Map("source_id" -> sourceId, "updated_at" -> updatedAt.inSeconds, "graph_id" -> graphId, "direction" -> direction.id, "priority" -> priority.id)

  def apply() {
    Stats.incr("multijobs-" + getClass.getName.split("\\.").last)
    var cursor = Cursor.Start
    val forwardShard = forwardingManager.find(sourceId, graphId, direction)
    try {
      updateMetadata(forwardShard)
    } catch {
      case e: ShardBlackHoleException =>
        return
    }
    val states = Seq(Normal, Archived, Negative) // Removed edges are never bulk-updated
    while (cursor != Cursor.End) {
      val resultWindow = forwardShard.selectEdges(sourceId, states, aggregateJobPageSize, cursor)

      val chunkOfTasks = resultWindow.map { edge =>
        val destinationId = edge.destinationId
        val (a, b) = if (direction == Direction.Backward) (destinationId, sourceId) else (sourceId, destinationId)
        val uuidGenerator(uuid) = edge.position
        update(a, graphId, b, uuid)
      }
      scheduler.put(priority.id, new JsonNestedJob(chunkOfTasks))

      cursor = resultWindow.nextCursor
    }
  }

  protected def update(sourceId: Long, graphId: Int, destinationId: Long, position: Long): JsonJob
  protected def updateMetadata(shard: Shard)
}

case class Archive(sourceId: Long, graphId: Int, direction: Direction, updatedAt: Time,
                   priority: Priority.Value, aggregateJobPageSize: Int, forwardingManager: ForwardingManager,
                   scheduler: PrioritizingJobScheduler, uuidGenerator: UuidGenerator)
           extends Multi(sourceId, graphId, direction, updatedAt, priority, aggregateJobPageSize, forwardingManager, scheduler, uuidGenerator) {
  protected def update(sourceId: Long, graphId: Int, destinationId: Long, position: Long) =
    new jobs.single.Archive(sourceId, graphId, destinationId, position, updatedAt, null, null)
  protected def updateMetadata(shard: Shard) = shard.archive(sourceId, updatedAt)
}

case class Unarchive(sourceId: Long, graphId: Int, direction: Direction, updatedAt: Time,
                     priority: Priority.Value, aggregateJobPageSize: Int, forwardingManager: ForwardingManager,
                      scheduler: PrioritizingJobScheduler, uuidGenerator: UuidGenerator)
           extends Multi(sourceId, graphId, direction, updatedAt, priority, aggregateJobPageSize, forwardingManager, scheduler, uuidGenerator) {
  protected def update(sourceId: Long, graphId: Int, destinationId: Long, position: Long) = {
    new jobs.single.Add(sourceId, graphId, destinationId, position, updatedAt, null, null)
  }
  protected def updateMetadata(shard: Shard) = shard.add(sourceId, updatedAt)
}

case class RemoveAll(sourceId: Long, graphId: Int, direction: Direction, updatedAt: Time,
                     priority: Priority.Value, aggregateJobPageSize: Int, forwardingManager: ForwardingManager,
                      scheduler: PrioritizingJobScheduler, uuidGenerator: UuidGenerator)
           extends Multi(sourceId, graphId, direction, updatedAt, priority, aggregateJobPageSize, forwardingManager, scheduler, uuidGenerator) {
  protected def update(sourceId: Long, graphId: Int, destinationId: Long, position: Long) =
    new jobs.single.Remove(sourceId, graphId, destinationId, position, updatedAt, null, null)
  protected def updateMetadata(shard: Shard) = shard.remove(sourceId, updatedAt)
}

case class Negate(sourceId: Long, graphId: Int, direction: Direction, updatedAt: Time,
                  priority: Priority.Value, aggregateJobPageSize: Int, forwardingManager: ForwardingManager,
                   scheduler: PrioritizingJobScheduler, uuidGenerator: UuidGenerator)
           extends Multi(sourceId, graphId, direction, updatedAt, priority, aggregateJobPageSize, forwardingManager, scheduler, uuidGenerator) {
  protected def update(sourceId: Long, graphId: Int, destinationId: Long, position: Long) =
    new jobs.single.Negate(sourceId, graphId, destinationId, position, updatedAt, null, null)
  protected def updateMetadata(shard: Shard) = shard.negate(sourceId, updatedAt)
}
