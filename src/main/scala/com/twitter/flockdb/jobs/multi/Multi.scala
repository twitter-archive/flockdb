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
import com.twitter.ostrich.Stats
import com.twitter.xrayspecs.Time
import com.twitter.xrayspecs.TimeConversions._
import net.lag.configgy.Configgy
import shards.Shard

abstract class MultiJobParser extends JsonJobParser[JsonJob] {
  def apply(attributes: Map[String, Any]): JsonJob = {
    val casted = attributes.asInstanceOf[Map[String, AnyVal]]
    createJob(
      casted("source_id").toLong,
      casted("graph_id").toInt,
      Direction(casted("direction").toInt),
      Time(casted("updated_at").toInt.seconds),
      Priority(casted.get("priority").map(_.toInt).getOrElse(Priority.Low.id)))
  }

  protected def createJob(sourceId: Long, graphId: Int, direction: Direction, updatedAt: Time, priority: Priority.Value): Multi
}

class ArchiveParser(forwardingManager: ForwardingManager, scheduler: PrioritizingJobScheduler[JsonJob]) extends MultiJobParser {
  protected def createJob(sourceId: Long, graphId: Int, direction: Direction, updatedAt: Time, priority: Priority.Value) = {
    new Archive(sourceId, graphId, direction, updatedAt, priority, forwardingManager, scheduler)
  }
}

class UnarchiveParser(forwardingManager: ForwardingManager, scheduler: PrioritizingJobScheduler[JsonJob]) extends MultiJobParser {
  protected def createJob(sourceId: Long, graphId: Int, direction: Direction, updatedAt: Time, priority: Priority.Value) = {
    new Unarchive(sourceId, graphId, direction, updatedAt, priority, forwardingManager, scheduler)
  }
}

class RemoveAllParser(forwardingManager: ForwardingManager, scheduler: PrioritizingJobScheduler[JsonJob]) extends MultiJobParser {
  protected def createJob(sourceId: Long, graphId: Int, direction: Direction, updatedAt: Time, priority: Priority.Value) = {
    new RemoveAll(sourceId, graphId, direction, updatedAt, priority, forwardingManager, scheduler)
  }
}

class NegateParser(forwardingManager: ForwardingManager, scheduler: PrioritizingJobScheduler[JsonJob]) extends MultiJobParser {
  protected def createJob(sourceId: Long, graphId: Int, direction: Direction, updatedAt: Time, priority: Priority.Value) = {
    new Negate(sourceId, graphId, direction, updatedAt, priority, forwardingManager, scheduler)
  }
}

abstract class Multi(sourceId: Long, graphId: Int, direction: Direction, updatedAt: Time,
               priority: Priority.Value, forwardingManager: ForwardingManager,
               scheduler: PrioritizingJobScheduler[JsonJob])
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
    while (cursor != Cursor.End) {
      val resultWindow = forwardShard.selectIncludingArchived(sourceId, config("edges.aggregate_jobs_page_size").toInt, cursor)

      val chunkOfTasks = resultWindow.map { destinationId =>
        val (a, b) = if (direction == Direction.Backward) (destinationId, sourceId) else (sourceId, destinationId)
        update(a, graphId, b)
      }
      scheduler.put(priority.id, new JsonNestedJob(chunkOfTasks))

      cursor = resultWindow.nextCursor
    }
  }

  protected def update(sourceId: Long, graphId: Int, destinationId: Long): JsonJob
  protected def updateMetadata(shard: Shard)
}

case class Archive(sourceId: Long, graphId: Int, direction: Direction, updatedAt: Time,
                   priority: Priority.Value, forwardingManager: ForwardingManager,
                   scheduler: PrioritizingJobScheduler[JsonJob])
           extends Multi(sourceId, graphId, direction, updatedAt, priority, forwardingManager, scheduler) {
  protected def update(sourceId: Long, graphId: Int, destinationId: Long) =
    new single.Archive(sourceId, graphId, destinationId, updatedAt.inMillis, updatedAt, null, null)
  protected def updateMetadata(shard: Shard) = shard.archive(sourceId, updatedAt)
}

case class Unarchive(sourceId: Long, graphId: Int, direction: Direction, updatedAt: Time,
                     priority: Priority.Value, forwardingManager: ForwardingManager,
                      scheduler: PrioritizingJobScheduler[JsonJob])
           extends Multi(sourceId, graphId, direction, updatedAt, priority, forwardingManager, scheduler) {
  protected def update(sourceId: Long, graphId: Int, destinationId: Long) =
    new single.Add(sourceId, graphId, destinationId, updatedAt.inMillis, updatedAt, null, null)
  protected def updateMetadata(shard: Shard) = shard.add(sourceId, updatedAt)
}

case class RemoveAll(sourceId: Long, graphId: Int, direction: Direction, updatedAt: Time,
                     priority: Priority.Value, forwardingManager: ForwardingManager,
                      scheduler: PrioritizingJobScheduler[JsonJob])
           extends Multi(sourceId, graphId, direction, updatedAt, priority, forwardingManager, scheduler) {
  protected def update(sourceId: Long, graphId: Int, destinationId: Long) =
    new single.Remove(sourceId, graphId, destinationId, updatedAt.inMillis, updatedAt, null, null)
  protected def updateMetadata(shard: Shard) = shard.remove(sourceId, updatedAt)
}

case class Negate(sourceId: Long, graphId: Int, direction: Direction, updatedAt: Time,
                  priority: Priority.Value, forwardingManager: ForwardingManager,
                   scheduler: PrioritizingJobScheduler[JsonJob])
           extends Multi(sourceId, graphId, direction, updatedAt, priority, forwardingManager, scheduler) {
  protected def update(sourceId: Long, graphId: Int, destinationId: Long) =
    new single.Negate(sourceId, graphId, destinationId, updatedAt.inMillis, updatedAt, null, null)
  protected def updateMetadata(shard: Shard) = shard.negate(sourceId, updatedAt)
}
