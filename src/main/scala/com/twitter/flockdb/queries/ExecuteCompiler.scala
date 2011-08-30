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
package queries

import scala.collection.mutable
import com.twitter.gizzard.scheduler.{JsonJob, JsonNestedJob, PrioritizingJobScheduler}
import com.twitter.gizzard.shards.ShardException
import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.util.Time
import com.twitter.util.TimeConversions._
import jobs.single.Single
import jobs.multi.Multi
import operations.{ExecuteOperations, ExecuteOperationType}


class ExecuteCompiler(scheduler: PrioritizingJobScheduler, forwardingManager: ForwardingManager, aggregateJobPageSize: Int) {
  @throws(classOf[ShardException])
  def apply(program: ExecuteOperations) {
    val now = Time.now
    val operations = program.operations
    val results = new mutable.ArrayBuffer[JsonJob]
    if (operations.size == 0) throw new InvalidQueryException("You must have at least one operation")

    for (op <- operations) {
      val term = op.term
      val time = program.executeAt.map(Time.fromSeconds).getOrElse(Time.now)
      val position = op.position.getOrElse(Time.now.inMillis)

      // force an exception for nonexistent graphs
      forwardingManager.find(0, term.graphId, Direction.Forward)

      val state = op.operationType match {
        case ExecuteOperationType.Add => State.Normal
        case ExecuteOperationType.Remove => State.Removed
        case ExecuteOperationType.Archive => State.Archived
        case ExecuteOperationType.Negate => State.Negative
        case n => throw new InvalidQueryException("Unknown operation " + n)
      }

      results ++= processDestinations(term) { (sourceId, destinationId) =>
        new Single(
          sourceId,
          term.graphId,
          destinationId,
          position,
          state,
          time,
          null,
          null
        )
      } {
        new Multi(
          term.sourceId,
          term.graphId,
          Direction(term.isForward),
          state,
          time,
          program.priority,
          aggregateJobPageSize,
          null,
          null
        )
      }
    }

    scheduler.put(program.priority.id, new JsonNestedJob(results))
  }

  private def processDestinations(term: QueryTerm)(handleItemInCollection: (Long, Long) => JsonJob)(noDestinations: JsonJob) = {
    if (term.destinationIds.isDefined) {
      for (d <- term.destinationIds.get) yield {
        val (sourceId, destinationId) = if (term.isForward) {
          (term.sourceId, d)
        } else {
          (d, term.sourceId)
        }
        handleItemInCollection(sourceId, destinationId)
      }
    } else {
      List(noDestinations)
    }
  }
}
