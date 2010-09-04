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

import com.twitter.flockdb.shards.Shard
import com.twitter.gizzard.jobs.{UnboundJobParser, UnboundJob}
import com.twitter.xrayspecs.Time
import com.twitter.xrayspecs.TimeConversions._
import net.lag.logging.Logger


abstract class SingleJobParser extends UnboundJobParser[(ForwardingManager, UuidGenerator)] {
  def apply(attributes: Map[String, Any]) = {
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

object AddParser extends SingleJobParser {
  protected def createJob(sourceId: Long, graphId: Int, destinationId: Long, position: Long, updatedAt: Time) = {
    new Add(sourceId, graphId, destinationId, position, updatedAt)
  }
}

object RemoveParser extends SingleJobParser {
  protected def createJob(sourceId: Long, graphId: Int, destinationId: Long, position: Long, updatedAt: Time) = {
    new Remove(sourceId, graphId, destinationId, position, updatedAt)
  }
}

object ArchiveParser extends SingleJobParser {
  protected def createJob(sourceId: Long, graphId: Int, destinationId: Long, position: Long, updatedAt: Time) = {
    new Archive(sourceId, graphId, destinationId, position, updatedAt)
  }
}

object NegateParser extends SingleJobParser {
  protected def createJob(sourceId: Long, graphId: Int, destinationId: Long, position: Long, updatedAt: Time) = {
    new Negate(sourceId, graphId, destinationId, position, updatedAt)
  }
}

abstract class Single(sourceId: Long, graphId: Int, destinationId: Long, position: Long, updatedAt: Time) extends UnboundJob[(ForwardingManager, UuidGenerator)] {
  def toMap = {
    Map("source_id" -> sourceId, "graph_id" -> graphId, "destination_id" -> destinationId, "position" -> position, "updated_at" -> updatedAt.inSeconds)
  }

  def shards(forwardingManager: ForwardingManager) = {
    val forwardShard = forwardingManager.find(sourceId, graphId, Direction.Forward)
    val backwardShard = forwardingManager.find(destinationId, graphId, Direction.Backward)
    (forwardShard, backwardShard)
  }

  private def withOptimisticLock(forwardShard: Shard, backwardShard: Shard, sourceId: Long, destinationId: Long)(f: State => Unit) {
    val initialState = forwardShard.getMetadata(sourceId).map(_.state).getOrElse(State.Normal) max backwardShard.getMetadata(destinationId).map(_.state).getOrElse(State.Normal)
    f(initialState)
    val endState = forwardShard.getMetadata(sourceId).map(_.state).getOrElse(State.Normal) max backwardShard.getMetadata(destinationId).map(_.state).getOrElse(State.Normal)
    if (endState != initialState) {
      throw new Exception("Lost optimistic lock on id " + sourceId + " / " + destinationId)
    }
  }

  def apply(environment: (ForwardingManager, UuidGenerator)) {
    val (forwardingManager, uuidGenerator) = environment
    val (forwardShard, backwardShard) = shards(forwardingManager)
    val uuid = uuidGenerator(position)
    withOptimisticLock(forwardShard, backwardShard, sourceId, destinationId) { initialState =>
      (initialState max preferredState) match {
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

  protected def preferredState: State
}

case class Add(sourceId: Long, graphId: Int, destinationId: Long, position: Long, updatedAt: Time) extends Single(sourceId, graphId, destinationId, position, updatedAt) {
  def preferredState = State.Normal
}

case class Remove(sourceId: Long, graphId: Int, destinationId: Long, position: Long, updatedAt: Time) extends Single(sourceId, graphId, destinationId, position, updatedAt) {
  def preferredState = State.Removed
}

case class Archive(sourceId: Long, graphId: Int, destinationId: Long, position: Long, updatedAt: Time) extends Single(sourceId, graphId, destinationId, position, updatedAt) {
  def preferredState = State.Archived
}

case class Negate(sourceId: Long, graphId: Int, destinationId: Long, position: Long, updatedAt: Time) extends Single(sourceId, graphId, destinationId, position, updatedAt) {
  def preferredState = State.Negative
}
