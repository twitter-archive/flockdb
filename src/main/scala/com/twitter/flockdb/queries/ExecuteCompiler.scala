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
import jobs.single
import jobs.multi
import operations.{ExecuteOperations, ExecuteOperationType}


class ExecuteCompiler(scheduler: PrioritizingJobScheduler[JsonJob], forwardingManager: ForwardingManager, aggregateJobPageSize: Int) {
  @throws(classOf[ShardException])
  def apply(program: ExecuteOperations) {
    val now = Time.now
    val operations = program.operations
    val results = new mutable.ArrayBuffer[JsonJob]
    if (operations.size == 0) throw new InvalidQueryException("You must have at least one operation")

    for (op <- operations) {
      val term = op.term
      val time = program.executeAt.map { x => Time(x.seconds) }.getOrElse(Time.now)
      val position = op.position.getOrElse(Time.now.inMillis)

      // force an exception for nonexistent graphs
      forwardingManager.find(0, term.graphId, Direction.Forward)

      results ++= (op.operationType match {
        case ExecuteOperationType.Add =>
          processDestinations(term) { (sourceId, destinationId) =>
            single.Add(sourceId, term.graphId, destinationId, position, time, null, null)
          } {
            multi.Unarchive(term.sourceId, term.graphId, Direction(term.isForward), time, program.priority, aggregateJobPageSize, null, null)
          }
        case ExecuteOperationType.Remove =>
          processDestinations(term) { (sourceId, destinationId) =>
            single.Remove(sourceId, term.graphId, destinationId, position, time, null, null)
          } {
            multi.RemoveAll(term.sourceId, term.graphId, Direction(term.isForward), time, program.priority, aggregateJobPageSize, null, null)
          }
        case ExecuteOperationType.Archive =>
          processDestinations(term) { (sourceId, destinationId) =>
            single.Archive(sourceId, term.graphId, destinationId, position, time, null, null)
          } {
            multi.Archive(term.sourceId, term.graphId, Direction(term.isForward), time, program.priority, aggregateJobPageSize, null, null)
          }
        case ExecuteOperationType.Negate =>
          processDestinations(term) { (sourceId, destinationId) =>
            single.Negate(sourceId, term.graphId, destinationId, position, time, null, null)
          } {
            multi.Negate(term.sourceId, term.graphId, Direction(term.isForward), time, program.priority, aggregateJobPageSize, null, null)
          }
        case n =>
          throw new InvalidQueryException("Unknown operation " + n)
      })
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
