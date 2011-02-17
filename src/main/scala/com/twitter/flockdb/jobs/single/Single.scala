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
package jobs.single

import com.twitter.gizzard.scheduler.{JsonJob, JsonJobParser}
import com.twitter.gizzard.shards.{ShardException, ShardBlackHoleException, ShardRejectedOperationException}
import com.twitter.util.Time
import com.twitter.conversions.time._
import conversions.Numeric._
import shards.Shard

case class NodePair(sourceId: Long, destinationId: Long)


abstract class SingleJobParser extends JsonJobParser {
  def apply(attributes: Map[String, Any]): JsonJob = {
    val casted = attributes.asInstanceOf[Map[String, AnyVal]]
    createJob(
      casted("source_id").toLong,
      casted("graph_id").toInt,
      casted("destination_id").toLong,
      casted("position").toLong,
      Time(casted("updated_at").toInt.seconds))
  }

  protected def createJob(sourceId: Long, graphId: Int, destinationId: Long, position: Long, updatedAt: Time): Single
}

class AddParser(forwardingManager: ForwardingManager, uuidGenerator: UuidGenerator) extends SingleJobParser {
  protected def createJob(sourceId: Long, graphId: Int, destinationId: Long, position: Long, updatedAt: Time) = {
    new Add(sourceId, graphId, destinationId, position, updatedAt, forwardingManager, uuidGenerator)
  }
}

class RemoveParser(forwardingManager: ForwardingManager, uuidGenerator: UuidGenerator) extends SingleJobParser {
  protected def createJob(sourceId: Long, graphId: Int, destinationId: Long, position: Long, updatedAt: Time) = {
    new Remove(sourceId, graphId, destinationId, position, updatedAt, forwardingManager, uuidGenerator)
  }
}

class ArchiveParser(forwardingManager: ForwardingManager, uuidGenerator: UuidGenerator) extends SingleJobParser {
  protected def createJob(sourceId: Long, graphId: Int, destinationId: Long, position: Long, updatedAt: Time) = {
    new Archive(sourceId, graphId, destinationId, position, updatedAt, forwardingManager, uuidGenerator)
  }
}

class NegateParser(forwardingManager: ForwardingManager, uuidGenerator: UuidGenerator) extends SingleJobParser {
  protected def createJob(sourceId: Long, graphId: Int, destinationId: Long, position: Long, updatedAt: Time) = {
    new Negate(sourceId, graphId, destinationId, position, updatedAt, forwardingManager, uuidGenerator)
  }
}

abstract class Single(sourceId: Long, graphId: Int, destinationId: Long, position: Long,
                      updatedAt: Time, forwardingManager: ForwardingManager, uuidGenerator: UuidGenerator)
extends JsonJob {
  def toMap = {
    Map("source_id" -> sourceId, "graph_id" -> graphId, "destination_id" -> destinationId, "position" -> position, "updated_at" -> updatedAt.inSeconds)
  }


  def apply() = {
    val forwardShard = forwardingManager.find(sourceId, graphId, Direction.Forward)
    val backwardShard = forwardingManager.find(destinationId, graphId, Direction.Backward)
    val uuid = uuidGenerator(position)

    forwardShard.optimistically(sourceId) { left =>
      backwardShard.optimistically(destinationId) { right =>
        write(forwardShard, backwardShard, uuid, left max right max preferredState)
      }
    }

  }

  def writeToShard(shard: Shard, sourceId: Long, destinationId: Long, uuid: Long, state: State) = {
    try {
      state match {
        case State.Normal =>
          shard.add(sourceId, destinationId, uuid, updatedAt)
        case State.Removed =>
          shard.remove(sourceId, destinationId, uuid, updatedAt)
        case State.Archived =>
          shard.archive(sourceId, destinationId, uuid, updatedAt)
        case State.Negative =>
          shard.negate(sourceId, destinationId, uuid, updatedAt)
      }

      None
    } catch {
      case e => Some(e)
    }
  }

  def write(forwardShard: Shard, backwardShard: Shard, uuid: Long, state: State) {
    val forwardErr  = writeToShard(forwardShard, sourceId, destinationId, uuid, state)
    val backwardErr = writeToShard(backwardShard, destinationId, sourceId, uuid, state)

    // just eat ShardBlackHoleExceptions for either way, but throw any other
    List(forwardErr, backwardErr).flatMap(_.toList).foreach {
      case e: ShardBlackHoleException => ()
      case e => throw e
    }
  }

  protected def preferredState: State
}

case class Add(sourceId: Long, graphId: Int, destinationId: Long, position: Long, updatedAt: Time,
               forwardingManager: ForwardingManager, uuidGenerator: UuidGenerator)
           extends Single(sourceId, graphId, destinationId, position, updatedAt, forwardingManager, uuidGenerator) {
  def preferredState = State.Normal
}

case class Remove(sourceId: Long, graphId: Int, destinationId: Long, position: Long, updatedAt: Time,
                  forwardingManager: ForwardingManager, uuidGenerator: UuidGenerator)
           extends Single(sourceId, graphId, destinationId, position, updatedAt, forwardingManager, uuidGenerator) {
  def preferredState = State.Removed
}

case class Archive(sourceId: Long, graphId: Int, destinationId: Long, position: Long, updatedAt: Time,
                   forwardingManager: ForwardingManager, uuidGenerator: UuidGenerator)
           extends Single(sourceId, graphId, destinationId, position, updatedAt, forwardingManager, uuidGenerator) {
  def preferredState = State.Archived
}

case class Negate(sourceId: Long, graphId: Int, destinationId: Long, position: Long, updatedAt: Time,
                  forwardingManager: ForwardingManager, uuidGenerator: UuidGenerator)
           extends Single(sourceId, graphId, destinationId, position, updatedAt, forwardingManager, uuidGenerator) {
  def preferredState = State.Negative
}
