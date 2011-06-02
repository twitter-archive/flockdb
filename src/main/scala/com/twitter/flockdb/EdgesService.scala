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
import com.twitter.gizzard.nameserver.{NameServer, NonExistentShard, InvalidShard}
import com.twitter.gizzard.scheduler.{CopyJobFactory, JsonJob, PrioritizingJobScheduler}
import com.twitter.gizzard.shards.{ShardBlackHoleException, ShardDatabaseTimeoutException,
  ShardOfflineException, ShardTimeoutException}
import com.twitter.gizzard.thrift.conversions.Sequences._
import operations.{ExecuteOperations, SelectOperation}
import com.twitter.ostrich.Stats
import queries._
import thrift.FlockException
import net.lag.logging.Logger

class EdgesService(val nameServer: NameServer[shards.Shard],
                   var forwardingManager: ForwardingManager,
                   val copyFactory: CopyJobFactory[shards.Shard],
                   val schedule: PrioritizingJobScheduler,
                   future: Future,
                   intersectionQueryConfig: config.IntersectionQuery,
                   aggregateJobsPageSize: Int,
                   uuidGenerator: UuidGenerator) {

  private val log = Logger.get(getClass.getName)
  private val selectCompiler = new SelectCompiler(forwardingManager, intersectionQueryConfig)
  private var executeCompiler = new ExecuteCompiler(schedule, forwardingManager, aggregateJobsPageSize, uuidGenerator)

  def shutdown() {
    schedule.shutdown()
    future.shutdown()
  }

  def containsMetadata(sourceId: Long, graphId: Int): Boolean = {
    rethrowExceptionsAsThrift {
      forwardingManager.find(sourceId, graphId, Direction.Forward).getMetadata(sourceId).isDefined
    }
  }

  def contains(sourceId: Long, graphId: Int, destinationId: Long): Boolean = {
    rethrowExceptionsAsThrift {
      forwardingManager.find(sourceId, graphId, Direction.Forward).get(sourceId, destinationId).map { edge =>
        edge.state == State.Normal || edge.state == State.Negative
      }.getOrElse(false)
    }
  }

  def get(sourceId: Long, graphId: Int, destinationId: Long): Edge = {
    rethrowExceptionsAsThrift {
      forwardingManager.find(sourceId, graphId, Direction.Forward).get(sourceId, destinationId).getOrElse {
        throw new FlockException("Record not found: (%d, %d, %d)".format(sourceId, graphId, destinationId))
      }
    }
  }

  def getMetadata(sourceId: Long, graphId: Int): Metadata = {
    rethrowExceptionsAsThrift {
      forwardingManager.find(sourceId, graphId, Direction.Forward).getMetadata(sourceId).getOrElse {
        throw new FlockException("Record not found: (%d, %d)".format(sourceId, graphId))
      }
    }
  }

  def select(query: SelectQuery): ResultWindow[Long] = select(List(query)).head

  def select(queries: Seq[SelectQuery]): Seq[ResultWindow[Long]] = {
    rethrowExceptionsAsThrift {
      queries.parallel(future).map { query =>
        try {
          selectCompiler(query.operations).select(query.page)
        } catch {
          case e: ShardBlackHoleException =>
            throw new FlockException("Shard is blackholed: " + e)
        }
      }
    }
  }

  def selectEdges(queries: Seq[EdgeQuery]): Seq[ResultWindow[Edge]] = {
    rethrowExceptionsAsThrift {
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
  }

  def execute(operations: ExecuteOperations) {
    rethrowExceptionsAsThrift {
      executeCompiler(operations)
    }
  }

  def count(queries: Seq[Seq[SelectOperation]]): Seq[Int] = {
    rethrowExceptionsAsThrift {
      queries.parallel(future).map { query =>
        selectCompiler(query).sizeEstimate
      }
    }
  }

  private def countAndRethrow(e: Throwable) = {
    Stats.incr("exceptions-" + e.getClass.getName.split("\\.").last)
    throw(new FlockException(e.getMessage))
  }

  private def rethrowExceptionsAsThrift[A](block: => A): A = {
    try {
      block
    } catch {
      case e: NonExistentShard =>
        log.error(e, "NonexistentShard: %s", e)
        throw(new FlockException(e.getMessage))
      case e: InvalidShard =>
        log.error(e, "NonexistentShard: %s", e)
        throw(new FlockException(e.getMessage))
      case e: FlockException =>
        Stats.incr(e.getClass.getName)
        throw(e)
      case e: ShardTimeoutException =>
        countAndRethrow(e)
      case e: ShardDatabaseTimeoutException =>
        countAndRethrow(e)
      case e: ShardOfflineException =>
        countAndRethrow(e)
      case e: Throwable =>
        Stats.incr("exceptions-unknown")
        log.error(e, "Unhandled error in EdgesService: %s", e)
        throw(new FlockException(e.toString))
    }
  }
}
