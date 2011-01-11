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

case class Edge(sourceId: Long, destinationId: Long, position: Long, updatedAt: Time, count: Int,
                state: State) extends Repairable[Edge] {

  def schedule(tableId: Int, forwardingManager: ForwardingManager, scheduler: PrioritizingJobScheduler[JsonJob], priority: Int) = {
    scheduler.put(priority, toJob(tableId, forwardingManager))
  }

  def toJob(tableId: Int, forwardingManager: ForwardingManager) = {
    val job = state match {
      case State.Normal => Add
      case State.Removed => Remove
      case State.Archived => Archive
      case State.Negative => Negate
    }
    job(sourceId, tableId, destinationId, position, updatedAt, forwardingManager, OrderedUuidGenerator)
  }

  def similar(other:Edge) = {
    sourceId.compare(other.sourceId) match {
      case x if x < 0 => -1
      case x if x > 0 => 1
      case _ => destinationId.compare(other.destinationId)
    }
  }
}