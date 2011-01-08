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

import com.twitter.gizzard.scheduler.{JsonJob, JsonJobParser}
import com.twitter.gizzard.shards.ShardBlackHoleException
import com.twitter.util.Time
import com.twitter.util.TimeConversions._
import conversions.Numeric._
import shards.Shard

class UnsafeAddJobParser(forwardingManager: ForwardingManager, uuidGenerator: UuidGenerator) extends JsonJobParser {
  def apply(attributes: Map[String, Any]): JsonJob = {
    val casted = attributes.asInstanceOf[Map[String, AnyVal]]
    new UnsafeAddJob(
      casted("source_id").toLong,
      casted("graph_id").toInt,
      casted("destination_id").toLong,
      casted("position").toLong,
      Time(casted("updated_at").toInt.seconds), forwardingManager, uuidGenerator)
  }
}

class UnsafeAddJob(sourceId: Long, graphId: Int, destinationId: Long, position: Long, updatedAt: Time,
                   forwardingManager: ForwardingManager, uuidGenerator: UuidGenerator) extends JsonJob {
  def toMap = {
    Map("source_id" -> sourceId, "graph_id" -> graphId, "destination_id" -> destinationId, "position" -> position, "updated_at" -> updatedAt.inSeconds)
  }

  def shards() = {
    val forwardShard = forwardingManager.find(sourceId, graphId, Direction.Forward)
    val backwardShard = forwardingManager.find(destinationId, graphId, Direction.Backward)
    (forwardShard, backwardShard)
  }

  def apply() = {
    val (forwardShard, backwardShard) = shards()
    val uuid = uuidGenerator(position)

    try {
      forwardShard.addUnsafe(sourceId, destinationId, uuid, updatedAt)
    } catch {
      case e: ShardBlackHoleException =>
    }
    try {
      backwardShard.addUnsafe(sourceId, destinationId, uuid, updatedAt)
    } catch {
      case e: ShardBlackHoleException =>
    }
  }
}

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

  def shards() = {
    val forwardShard = forwardingManager.find(sourceId, graphId, Direction.Forward)
    val backwardShard = forwardingManager.find(destinationId, graphId, Direction.Backward)
    (forwardShard, backwardShard)
  }

  private def withLock(state: State, shard: Shard, id: Long)(f: (State, Option[Shard]) => Unit) {
    try {
      shard.withLock(id) { (newShard, metadata) =>
        f(metadata.state max state, Some(newShard))
      }
    } catch {
      case e: ShardBlackHoleException =>
        f(state, None)
    }
  }

  def apply() {
    val (forwardShard, backwardShard) = shards()
    val uuid = uuidGenerator(position)
    withLock(preferredState, forwardShard, sourceId) { (state, forwardShard) =>
      withLock(state, backwardShard, destinationId) { (state, backwardShard) =>
        state match {
          case State.Normal =>
            forwardShard.foreach { _.add(sourceId, destinationId, uuid, updatedAt) }
            backwardShard.foreach { _.add(destinationId, sourceId, uuid, updatedAt) }
          case State.Removed =>
            forwardShard.foreach { _.remove(sourceId, destinationId, uuid, updatedAt) }
            backwardShard.foreach { _.remove(destinationId, sourceId, uuid, updatedAt) }
          case State.Archived =>
            forwardShard.foreach { _.archive(sourceId, destinationId, uuid, updatedAt) }
            backwardShard.foreach { _.archive(destinationId, sourceId, uuid, updatedAt) }
          case State.Negative =>
            forwardShard.foreach { _.negate(sourceId, destinationId, uuid, updatedAt) }
            backwardShard.foreach { _.negate(destinationId, sourceId, uuid, updatedAt) }
        }
      }
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
