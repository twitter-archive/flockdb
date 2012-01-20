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

import com.twitter.util.Time
import com.twitter.flockdb.jobs.single._
import com.twitter.gizzard.scheduler.{PrioritizingJobScheduler, JsonJob}

object Edge {
  def apply(sourceId: Long, destinationId: Long, position: Long, updatedAt: Time, count: Int, state: State) = new Edge(sourceId, destinationId, position, updatedAt, count, state)
  val Max = Edge(Long.MaxValue, Long.MaxValue, Long.MaxValue, 0, 0, State.Normal)
}

case class Edge(sourceId: Long, destinationId: Long, position: Long, updatedAtSeconds: Int, count: Int,
                state: State) extends Ordered[Edge] {

  def this(sourceId: Long, destinationId: Long, position: Long, updatedAt: Time, count: Int, state: State) =
    this(sourceId, destinationId, position, updatedAt.inSeconds, count, state)

  val updatedAt = Time.fromSeconds(updatedAtSeconds)

  def schedule(tableId: Int, forwardingManager: ForwardingManager, scheduler: PrioritizingJobScheduler, priority: Int) = {
    scheduler.put(priority, toJob(tableId, forwardingManager))
  }

  def toJob(tableId: Int, forwardingManager: ForwardingManager) = {
    new Single(
      sourceId,
      tableId,
      destinationId,
      OrderedUuidGenerator.unapply(position).get,
      state,
      updatedAt,
      forwardingManager,
      OrderedUuidGenerator
    )
  }

  def similar(other:Edge) = {
    sourceId.compare(other.sourceId) match {
      case x if x < 0 => -1
      case x if x > 0 => 1
      case _ => destinationId.compare(other.destinationId)
    }
  }

  def compare(other: Edge) = {
    val out = updatedAt.compare(other.updatedAt)
    if (out == 0) {
      state.compare(other.state)
    } else {
      out
    }
  }

  def max(other: Edge) = if (this > other) this else other
}
