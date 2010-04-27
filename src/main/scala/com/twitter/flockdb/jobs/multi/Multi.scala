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

import com.twitter.gizzard.jobs.{BoundJobParser, Schedulable,
  SchedulableWithTasks, UnboundJob}
import com.twitter.gizzard.scheduler.PrioritizingJobScheduler
import com.twitter.results.Cursor
import com.twitter.xrayspecs.Time
import com.twitter.xrayspecs.TimeConversions._
import net.lag.configgy.Configgy
import flockdb.shards.Shard


class JobParser(forwardingManager: ForwardingManager, scheduler: PrioritizingJobScheduler) extends BoundJobParser((forwardingManager, scheduler))

abstract class Multi(sourceId: Long, graphId: Int, direction: Direction, updatedAt: Time, priority: Priority.Value) extends UnboundJob[(ForwardingManager, PrioritizingJobScheduler)] {
  private val config = Configgy.config

  def toMap = Map("source_id" -> sourceId, "updated_at" -> updatedAt.inSeconds, "graph_id" -> graphId, "direction" -> direction.id, "priority" -> priority.id)

  def apply(environment: (ForwardingManager, PrioritizingJobScheduler)) {
    val (forwardingManager, scheduler) = environment
    var cursor = Cursor.Start
    val forwardShard = forwardingManager.find(sourceId, graphId, direction)
    updateMetadata(forwardShard)
    while (cursor != Cursor.End) {
      val resultWindow = forwardShard.selectIncludingArchived(sourceId, config("edges.aggregate_jobs_page_size").toInt, cursor)

      val chunkOfTasks = resultWindow.map { destinationId =>
        val (a, b) = if (direction == Direction.Backward) (destinationId, sourceId) else (sourceId, destinationId)
        update(a, graphId, b)
      }
      scheduler(priority.id, new SchedulableWithTasks(chunkOfTasks))

      cursor = resultWindow.nextCursor
    }
  }

  protected def update(sourceId: Long, graphId: Int, destinationId: Long): Schedulable
  protected def updateMetadata(shard: Shard)
}

case class Archive(sourceId: Long, graphId: Int, direction: Direction, updatedAt: Time, priority: Priority.Value) extends Multi(sourceId, graphId, direction, updatedAt, priority) {
  def this(attributes: Map[String, AnyVal]) = this(
    attributes("source_id").toLong,
    attributes("graph_id").toInt,
    Direction(attributes("direction").toInt),
    Time(attributes("updated_at").toInt.seconds),
    Priority(attributes.get("priority").map(_.toInt).getOrElse(Priority.Low.id)))

  protected def update(sourceId: Long, graphId: Int, destinationId: Long) = single.Archive(sourceId, graphId, destinationId, updatedAt.inMillis, updatedAt)
  protected def updateMetadata(shard: Shard) = shard.archive(sourceId, updatedAt)
}

case class Unarchive(sourceId: Long, graphId: Int, direction: Direction, updatedAt: Time, priority: Priority.Value) extends Multi(sourceId, graphId, direction, updatedAt, priority) {
  def this(attributes: Map[String, AnyVal]) = this(
    attributes("source_id").toLong,
    attributes("graph_id").toInt,
    Direction(attributes("direction").toInt),
    Time(attributes("updated_at").toInt.seconds),
    Priority(attributes.get("priority").map(_.toInt).getOrElse(Priority.Low.id)))

  protected def update(sourceId: Long, graphId: Int, destinationId: Long) = new single.Add(sourceId, graphId, destinationId, updatedAt.inMillis, updatedAt)
  protected def updateMetadata(shard: Shard) = shard.add(sourceId, updatedAt)
}

case class RemoveAll(sourceId: Long, graphId: Int, direction: Direction, updatedAt: Time, priority: Priority.Value) extends Multi(sourceId, graphId, direction, updatedAt, priority) {
  def this(attributes: Map[String, AnyVal]) = this(
    attributes("source_id").toLong,
    attributes("graph_id").toInt,
    Direction(attributes("direction").toInt),
    Time(attributes("updated_at").toInt.seconds),
    Priority(attributes.get("priority").map(_.toInt).getOrElse(Priority.Low.id)))

  protected def update(sourceId: Long, graphId: Int, destinationId: Long) = new single.Remove(sourceId, graphId, destinationId, updatedAt.inMillis, updatedAt)
  protected def updateMetadata(shard: Shard) = shard.remove(sourceId, updatedAt)
}

case class Negate(sourceId: Long, graphId: Int, direction: Direction, updatedAt: Time, priority: Priority.Value) extends Multi(sourceId, graphId, direction, updatedAt, priority) {
  def this(attributes: Map[String, AnyVal]) = this(
    attributes("source_id").toLong,
    attributes("graph_id").toInt,
    Direction(attributes("direction").toInt),
    Time(attributes("updated_at").toInt.seconds),
    Priority(attributes.get("priority").map(_.toInt).getOrElse(Priority.Low.id)))

  protected def update(sourceId: Long, graphId: Int, destinationId: Long) = new single.Negate(sourceId, graphId, destinationId, updatedAt.inMillis, updatedAt)
  protected def updateMetadata(shard: Shard) = shard.negate(sourceId, updatedAt)
}
