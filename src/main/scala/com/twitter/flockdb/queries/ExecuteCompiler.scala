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

package com.twitter.flockdb.queries

import scala.collection.mutable
import com.twitter.gizzard.jobs.{Schedulable, SchedulableWithTasks}
import com.twitter.gizzard.scheduler.PrioritizingJobScheduler
import com.twitter.gizzard.shards.ShardException
import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.xrayspecs.Time
import com.twitter.xrayspecs.TimeConversions._
import jobs.single
import jobs.multi
import flockdb.operations.{ExecuteOperations, ExecuteOperationType}


class ExecuteCompiler(schedule: PrioritizingJobScheduler, forwardingManager: ForwardingManager) {
  @throws(classOf[ShardException])
  def apply(program: ExecuteOperations) {
    val now = Time.now
    val operations = program.operations
    val results = new mutable.ArrayBuffer[Schedulable]
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
            single.Add(sourceId, term.graphId, destinationId, position, time)
          } {
            multi.Unarchive(term.sourceId, term.graphId, Direction(term.isForward), time, program.priority)
          }
        case ExecuteOperationType.Remove =>
          processDestinations(term) { (sourceId, destinationId) =>
            single.Remove(sourceId, term.graphId, destinationId, position, time)
          } {
            multi.RemoveAll(term.sourceId, term.graphId, Direction(term.isForward), time, program.priority)
          }
        case ExecuteOperationType.Archive =>
          processDestinations(term) { (sourceId, destinationId) =>
            single.Archive(sourceId, term.graphId, destinationId, position, time)
          } {
            multi.Archive(term.sourceId, term.graphId, Direction(term.isForward), time, program.priority)
          }
        case ExecuteOperationType.Negate =>
          processDestinations(term) { (sourceId, destinationId) =>
            single.Negate(sourceId, term.graphId, destinationId, position, time)
          } {
            multi.Negate(term.sourceId, term.graphId, Direction(term.isForward), time, program.priority)
          }
        case n =>
          throw new InvalidQueryException("Unknown operation " + n)
      })
    }
    schedule(program.priority.id, new SchedulableWithTasks(results))
  }

  private def processDestinations(term: QueryTerm)(handleItemInCollection: (Long, Long) => Schedulable)(noDestinations: Schedulable) = {
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
