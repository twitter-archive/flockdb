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

package com.twitter.flockdb.jobs.single

import com.twitter.logging.Logger
import com.twitter.util.{Time, Return, Throw}
import com.twitter.gizzard.scheduler._
import com.twitter.gizzard.shards._
import com.twitter.conversions.time._
import com.twitter.flockdb.{State, ForwardingManager, Cursor, UuidGenerator, Direction}
import com.twitter.flockdb.conversions.Numeric._
import com.twitter.flockdb.shards.Shard
import com.twitter.flockdb.shards.LockingNodeSet._


// TODO: Make this async.
class SingleJobParser(
  forwardingManager: ForwardingManager,
  uuidGenerator: UuidGenerator)
extends JsonJobParser {

  def log = Logger.get

  def apply(attributes: Map[String, Any]): JsonJob = {
    val writeSuccesses = try {
      attributes.get("write_successes") map {
        _.asInstanceOf[Seq[Seq[String]]] map { case Seq(h, tp) => ShardId(h, tp) }
      } getOrElse Nil
    } catch {
      case e => {
        log.warning("Error parsing write successes. falling back to non-memoization", e)
        Nil
      }
    }

    val casted = attributes.asInstanceOf[Map[String, AnyVal]]

    new Single(
      casted("source_id").toLong,
      casted("graph_id").toInt,
      casted("destination_id").toLong,
      casted("position").toLong,
      State(casted("state").toInt),
      Time.fromSeconds(casted("updated_at").toInt),
      forwardingManager,
      uuidGenerator,
      writeSuccesses.toList
    )
  }
}

class Single(
  sourceId: Long,
  graphId: Int,
  destinationId: Long,
  position: Long,
  preferredState: State,
  updatedAt: Time,
  forwardingManager: ForwardingManager,
  uuidGenerator: UuidGenerator,
  var successes: List[ShardId] = Nil)
extends JsonJob {

  def toMap = {
    val base =  Map(
      "source_id" -> sourceId,
      "graph_id" -> graphId,
      "destination_id" -> destinationId,
      "position" -> position,
      "state" -> preferredState.id,
      "updated_at" -> updatedAt.inSeconds
    )

    if (successes.isEmpty) {
      base
    } else {
      base + ("write_successes" -> (successes map { case ShardId(h, tp) => Seq(h, tp) }))
    }
  }

  def apply() = {
    val forward  = forwardingManager.findNode(sourceId, graphId, Direction.Forward).write
    val backward = forwardingManager.findNode(destinationId, graphId, Direction.Backward).write
    val uuid     = uuidGenerator(position)

    var currSuccesses: List[ShardId] = Nil
    var currErrs: List[Throwable]    = Nil

    forward.optimistically(sourceId) { left =>
      backward.optimistically(destinationId) { right =>
        val state           = left max right max preferredState
        val forwardResults  = writeToShard(forward, sourceId, destinationId, uuid, state)
        val backwardResults = writeToShard(backward, destinationId, sourceId, uuid, state)

        List(forwardResults, backwardResults) foreach {
          _ foreach {
            case Return(id) => currSuccesses = id :: currSuccesses
            case Throw(e)   => currErrs = e :: currErrs
          }
        }
      }
    }

    // add successful writes here, since we are only successful if an optimistic lock exception is not raised.
    successes = successes ++ currSuccesses

    currErrs.headOption foreach { e => throw e }
  }

  def writeToShard(shards: NodeSet[Shard], sourceId: Long, destinationId: Long, uuid: Long, state: State) = {
    shards.skip(successes) all { (shardId, shard) =>
      state match {
        case State.Normal   => shard.add(sourceId, destinationId, uuid, updatedAt)()
        case State.Removed  => shard.remove(sourceId, destinationId, uuid, updatedAt)()
        case State.Archived => shard.archive(sourceId, destinationId, uuid, updatedAt)()
        case State.Negative => shard.negate(sourceId, destinationId, uuid, updatedAt)()
      }

      shardId
    }
  }

  override def equals(o: Any) = o match {
    case o: Single => this.toMap == o.toMap
    case _         => false
  }
}
