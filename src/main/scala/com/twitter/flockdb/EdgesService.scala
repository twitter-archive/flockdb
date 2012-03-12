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

import com.twitter.logging.Logger
import com.twitter.gizzard.Stats
import com.twitter.gizzard.nameserver.{NameServer, NonExistentShard, InvalidShard}
import com.twitter.gizzard.scheduler.{CopyJobFactory, JsonJob, PrioritizingJobScheduler}
import com.twitter.gizzard.shards._
import com.twitter.flockdb.operations.{ExecuteOperations, SelectOperation}
import com.twitter.flockdb.queries._
import com.twitter.flockdb.thrift.FlockException
import com.twitter.util.Future

class EdgesService(
  forwardingManager: ForwardingManager,
  schedule: PrioritizingJobScheduler,
  intersectionQueryConfig: config.IntersectionQuery,
  aggregateJobsPageSize: Int) {

  // TODO: Make serverName configurable.
  private val serverName = "edges"
  private val log = Logger.get(getClass.getName)
  private val exceptionLog = Logger.get("exception")
  private val selectCompiler = new SelectCompiler(forwardingManager, intersectionQueryConfig)
  private var executeCompiler = new ExecuteCompiler(schedule, forwardingManager, aggregateJobsPageSize)

  def containsMetadata(sourceId: Long, graphId: Int): Future[Boolean] = {
    wrapRPC("contains_metadata") {
      val name = "contains-metadata"
      Stats.transaction.name = name
      Stats.incr(name + "-graph_" + graphId + "-count")
      forwardingManager.find(sourceId, graphId, Direction.Forward).getMetadata(sourceId) map { _.isDefined }
    }
  }

  def contains(sourceId: Long, graphId: Int, destinationId: Long): Future[Boolean] = {
    wrapRPC("contains") {
      val name = "contains"
      Stats.transaction.name = name
      Stats.incr(name + "-graph_" + graphId + "-count")
      forwardingManager.find(sourceId, graphId, Direction.Forward).get(sourceId, destinationId) map {
        _ map { edge => edge.state == State.Normal || edge.state == State.Negative } getOrElse false
      }
    }
  }

  def get(sourceId: Long, graphId: Int, destinationId: Long): Future[Edge] = {
    wrapRPC("get") {
      val name = "get"
      Stats.transaction.name = name
      Stats.incr(name + "-graph_" + graphId + "-count")
      forwardingManager.find(sourceId, graphId, Direction.Forward).get(sourceId, destinationId) flatMap {
        case Some(edge) => Future(edge)
        case _ => Future.exception(new FlockException("Record not found: (%d, %d, %d)".format(sourceId, graphId, destinationId)))
      }
    }
  }

  def getMetadata(sourceId: Long, graphId: Int): Future[Metadata] = {
    wrapRPC("get_metadata") {
      val name = "get-metadata"
      Stats.transaction.name = name
      Stats.incr(name + "-graph_" + graphId + "-count")
      forwardingManager.find(sourceId, graphId, Direction.Forward).getMetadata(sourceId) flatMap {
        case Some(metadata) => Future(metadata)
        case _ => Future.exception(new FlockException("Record not found: (%d, %d)".format(sourceId, graphId)))
      }
    }
  }

  def select(query: SelectQuery): Future[ResultWindow[Long]] = select(List(query)) map { _.head }

  def select(queries: Seq[SelectQuery]): Future[Seq[ResultWindow[Long]]] = {
    wrapRPC("select") {
      Future.collect(queries map { query =>
        val queryTree = selectCompiler(query.operations)
        queryTree.select(query.page) onSuccess { _ =>
          Stats.transaction.record(queryTree.toString)
        } rescue {
          case e: ShardBlackHoleException =>
            Future.exception(new FlockException("Shard is blackholed: " + e))
        }
      })
    }
  }

  def selectEdges(queries: Seq[EdgeQuery]): Future[Seq[ResultWindow[Edge]]] = {
    wrapRPC("select_edges") {
      Future.collect(queries map { query =>
        val term = query.term
        Stats.incr("select-edge-graph_" + (if (term.isForward) "" else "n") + term.graphId + "-count")
        val shard = forwardingManager.find(term.sourceId, term.graphId, Direction(term.isForward))
        val states = if (term.states.isEmpty) List(State.Normal) else term.states

        if (term.destinationIds.isDefined) {
          shard.intersectEdges(term.sourceId, states, term.destinationIds.get) map { results =>
            new ResultWindow(results.map { edge => (edge, Cursor(edge.destinationId)) }, query.page.count, query.page.cursor)
          }
        } else {
          shard.selectEdges(term.sourceId, states, query.page.count, query.page.cursor)
        }
      })
    }
  }

  def execute(operations: ExecuteOperations): Future[Unit] = {
    wrapRPC("execute") {
      Stats.transaction.name = "execute"
      // TODO: This results in a kestrel enqueue, which can block on disk I/O. Consider moving this work
      // to a separate threadpool.
      executeCompiler(operations)
      Future.Unit
    }
  }

  def count(queries: Seq[Seq[SelectOperation]]): Future[Seq[Int]] = {
    wrapRPC("count") {
      Future.collect(queries map { query =>
        val queryTree = selectCompiler(query)
        queryTree.sizeEstimate onSuccess { _ =>
          Stats.transaction.record(queryTree.toString)
        }
      })
    }
  }

  private[this] def logAndWrapException(rpcName: String, e: Throwable) = {
    val endpoint = serverName +"/"+ rpcName
    e match {
      case e: NonExistentShard =>
        Stats.incr("nonexistent_shard_error_count")
        log.error(e, "Nonexistent shard: %s", e)
      case e: InvalidShard =>
        Stats.incr("invalid_shard_error_count")
        log.error(e, "Invalid shard: %s", e)
      case e: FlockException =>
        Stats.incr("normal_error_count_" + endpoint)
      case e: ShardDatabaseTimeoutException =>
          Stats.incr("timeout_count_" + endpoint)
      case e: ShardTimeoutException =>
        Stats.incr("timeout_count_" + endpoint)
      case e: ShardOfflineException =>
        Stats.incr("offline_count_" + endpoint)
      case _ =>
        Stats.incr("internal_error_count_" + endpoint)
        exceptionLog.error(e, "Unhandled error in EdgesService", e)
        log.error("Unhandled error in EdgesService: " + e.toString)
    }

    e match {
      case e: FlockException => Future.exception(e)
      case _ => Future.exception(new FlockException("%s: %s".format(e.getClass.getName, e.getMessage)))
    }
  }

  private[this] def timeFuture[T](label: String)(f: => Future[T]) = {
    Stats.timeFutureMillis(serverName +"/"+ label)(f)
  }

  private[this] def wrapRPC[T](rpcName: String)(f: => Future[T]) = timeFuture(rpcName) {
    val rv = try {
      f
    } catch {
      case e => Future.exception(e)
    }

    rv respond { _ =>
      Stats.incr(serverName +"/"+ rpcName +"_count")
    } onSuccess { _ =>
      Stats.incr(serverName +"/"+ rpcName +"_success_count")
    } rescue {
      case e => logAndWrapException(rpcName, e)
    }
  }
}
