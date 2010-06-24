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

import com.twitter.gizzard.jobs.{BoundJobParser, UnboundJob}
import com.twitter.xrayspecs.Time
import com.twitter.xrayspecs.TimeConversions._
import net.lag.logging.Logger


class JobParser(forwardingManager: ForwardingManager, uuidGenerator: UuidGenerator) extends BoundJobParser((forwardingManager, uuidGenerator))

abstract class Single(sourceId: Long, graphId: Int, destinationId: Long, position: Long, updatedAt: Time) extends UnboundJob[(ForwardingManager, UuidGenerator)] {
  def toMap = {
    Map("source_id" -> sourceId, "graph_id" -> graphId, "destination_id" -> destinationId, "position" -> position, "updated_at" -> updatedAt.inSeconds)
  }

  def shards(forwardingManager: ForwardingManager) = {
    val forwardShard = forwardingManager.find(sourceId, graphId, Direction.Forward)
    val backwardShard = forwardingManager.find(destinationId, graphId, Direction.Backward)
    (forwardShard, backwardShard)
  }

  def apply(environment: (ForwardingManager, UuidGenerator)) {
    val (forwardingManager, uuidGenerator) = environment
    val (forwardShard, backwardShard) = shards(forwardingManager)
    val uuid = uuidGenerator(position)
    forwardShard.withLock(sourceId) { (forwardShard, forwardMetadata) =>
      backwardShard.withLock(destinationId) { (backwardShard, backwardMetadata) =>
        val finalState = forwardMetadata.state max backwardMetadata.state max preferredState
        finalState match {
          case State.Normal =>
            forwardShard.add(sourceId, destinationId, uuid, updatedAt)
            backwardShard.add(destinationId, sourceId, uuid, updatedAt)
          case State.Removed =>
            forwardShard.remove(sourceId, destinationId, uuid, updatedAt)
            backwardShard.remove(destinationId, sourceId, uuid, updatedAt)
          case State.Archived =>
            forwardShard.archive(sourceId, destinationId, uuid, updatedAt)
            backwardShard.archive(destinationId, sourceId, uuid, updatedAt)
          case State.Negative =>
            forwardShard.negate(sourceId, destinationId, uuid, updatedAt)
            backwardShard.negate(destinationId, sourceId, uuid, updatedAt)
        }
      }
    }
  }

  protected def preferredState: State
}

case class Add(sourceId: Long, graphId: Int, destinationId: Long, position: Long, updatedAt: Time) extends Single(sourceId, graphId, destinationId, position, updatedAt) {
  def this(attributes: Map[String, AnyVal]) = {
    this(
      attributes("source_id").toLong,
      attributes("graph_id").toInt,
      attributes("destination_id").toLong,
      attributes("position").toLong,
      Time(attributes("updated_at").toInt.seconds))
  }

  def preferredState = State.Normal
}

case class Remove(sourceId: Long, graphId: Int, destinationId: Long, position: Long, updatedAt: Time) extends Single(sourceId, graphId, destinationId, position, updatedAt) {
  def this(attributes: Map[String, AnyVal]) = this(
    attributes("source_id").toLong,
    attributes("graph_id").toInt,
    attributes("destination_id").toLong,
    attributes("position").toLong,
    Time(attributes("updated_at").toInt.seconds))

  def preferredState = State.Removed
}

case class Archive(sourceId: Long, graphId: Int, destinationId: Long, position: Long, updatedAt: Time) extends Single(sourceId, graphId, destinationId, position, updatedAt) {
  def this(attributes: Map[String, AnyVal]) = this(
    attributes("source_id").toLong,
    attributes("graph_id").toInt,
    attributes("destination_id").toLong,
    attributes("position").toLong,
    Time(attributes("updated_at").toInt.seconds))

  def preferredState = State.Archived
}

case class Negate(sourceId: Long, graphId: Int, destinationId: Long, position: Long, updatedAt: Time) extends Single(sourceId, graphId, destinationId, position, updatedAt) {
  def this(attributes: Map[String, AnyVal]) = this(
    attributes("source_id").toLong,
    attributes("graph_id").toInt,
    attributes("destination_id").toLong,
    attributes("position").toLong,
    Time(attributes("updated_at").toInt.seconds))

  def preferredState = State.Negative
}
