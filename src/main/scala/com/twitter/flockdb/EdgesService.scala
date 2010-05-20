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

import com.twitter.gizzard.Future
import com.twitter.gizzard.jobs.CopyFactory
import com.twitter.gizzard.nameserver.NameServer
import com.twitter.gizzard.scheduler.PrioritizingJobScheduler
import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.results.{Cursor, ResultWindow}
import operations.{ExecuteOperations, SelectOperation}
import queries._
import thrift.FlockException


class EdgesService(val nameServer: NameServer[shards.Shard],
                   val forwardingManager: ForwardingManager,
                   val copyFactory: CopyFactory[shards.Shard],
                   val schedule: PrioritizingJobScheduler,
                   future: Future, replicationFuture: Future) {
  private val selectCompiler = new SelectCompiler(forwardingManager)
  private val executeCompiler = new ExecuteCompiler(schedule)

  def shutdown() {
    schedule.shutdown()
    future.shutdown()
    replicationFuture.shutdown()
  }

  def contains(sourceId: Long, graphId: Int, destinationId: Long): Boolean = {
    forwardingManager.find(sourceId, graphId, Direction.Forward).get(sourceId, destinationId).map { edge =>
      edge.state == State.Normal || edge.state == State.Negative
    }.getOrElse(false)
  }

  def get(sourceId: Long, graphId: Int, destinationId: Long): Edge = {
    forwardingManager.find(sourceId, graphId, Direction.Forward).get(sourceId, destinationId).getOrElse {
      throw new FlockException("Record not found: (%d, %d, %d)".format(sourceId, graphId, destinationId))
    }
  }

  def select(queries: Seq[SelectQuery]): Seq[ResultWindow[Long]] = {
    queries.parallel(future).map { query =>
      selectCompiler(query.operations).select(query.page)
    }
  }

  def selectEdges(queries: Seq[EdgeQuery]): Seq[ResultWindow[Edge]] = {
    queries.parallel(future).map { query =>
      val term = query.term
      val shard = forwardingManager.find(term.sourceId, term.graphId, Direction(term.isForward))
      val states = if (term.states.isEmpty) List(State.Normal) else term.states

      if (term.destinationIds.isDefined) {
        val results = shard.intersectEdges(term.sourceId, states, term.destinationIds.get)
        new ResultWindow(results.map { edge => (edge, Cursor(edge.destinationId)) }, query.page.count, query.page.cursor)
      } else {
        shard.selectEdges(term.sourceId, states, query.page.count, query.page.cursor)
      }
    }
  }

  def execute(operations: ExecuteOperations) {
    executeCompiler(operations)
  }

  def count(queries: Seq[Seq[SelectOperation]]): Seq[Int] = {
    queries.parallel(future).map { query =>
      selectCompiler(query).sizeEstimate
    }
  }
}
