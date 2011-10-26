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
      casted.get("wrote_forward_direction") map { case b: Boolean => b == true } getOrElse(false),
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
  var wroteForwardDirection: Boolean = false,
  var successes: List[ShardId] = Nil
)
extends JsonJob {
  def toMap = Map(
    "source_id" -> sourceId,
    "graph_id" -> graphId,
    "destination_id" -> destinationId,
    "position" -> position,
    "state" -> preferredState.id,
    "updated_at" -> updatedAt.inSeconds,
    "wrote_forward_direction" -> wroteForwardDirection,
    "write_successes" -> (successes map { case ShardId(h, tp) => Seq(h, tp) })
  )

  def apply() {
    val forward  = forwardingManager.findNode(sourceId, graphId, Direction.Forward).write
    val backward = forwardingManager.findNode(destinationId, graphId, Direction.Backward).write
    val uuid     = uuidGenerator(position)

    var currSuccesses: List[ShardId] = Nil
    var currErrs: List[Throwable]    = Nil

    // skip if we've successfully done this before.
    if (!wroteForwardDirection) {
      backward.optimistically(destinationId) { right =>
        writeToShard(forward, sourceId, destinationId, uuid, preferredState max right) foreach {
          case Return(id) => currSuccesses = id :: currSuccesses
          case Throw(e)   => currErrs = e :: currErrs
        }
      }
    }

    // add successful writes here, since we are only successful if an optimistic lock exception is not raised.
    successes = successes ++ currSuccesses

    // if there were no errors, then do not attempt a forward write again in the case of failure below.
    if (currErrs.isEmpty) wroteForwardDirection = true

    // reset for next direction
    currSuccesses = Nil

    forward.optimistically(sourceId) { left =>
      writeToShard(backward, destinationId, sourceId, uuid, preferredState max left) foreach {
        case Return(id) => currSuccesses = id :: currSuccesses
        case Throw(e)   => currErrs = e :: currErrs
      }
    }

    // add successful writes here, since we are only successful if an optimistic lock exception is not raised.
    successes = successes ++ currSuccesses

    currErrs.headOption foreach { e => throw e }
  }

  def writeToShard(shards: NodeSet[Shard], sourceId: Long, destinationId: Long, uuid: Long, state: State) = {
    shards.skip(successes) all { (shardId, shard) =>
      state match {
        case State.Normal   => shard.add(sourceId, destinationId, uuid, updatedAt)
        case State.Removed  => shard.remove(sourceId, destinationId, uuid, updatedAt)
        case State.Archived => shard.archive(sourceId, destinationId, uuid, updatedAt)
        case State.Negative => shard.negate(sourceId, destinationId, uuid, updatedAt)
      }

      shardId
    }
  }

  override def equals(o: Any) = o match {
    case o: Single => this.toMap == o.toMap
    case _         => false
  }
}
