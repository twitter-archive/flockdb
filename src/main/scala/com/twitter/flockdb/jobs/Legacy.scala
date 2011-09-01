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

package com.twitter.flockdb.jobs

import com.twitter.logging.Logger
import com.twitter.util.Time
import com.twitter.gizzard.scheduler._
import com.twitter.gizzard.shards._
import com.twitter.flockdb.{State, ForwardingManager, Cursor, UuidGenerator, Direction, Priority}
import com.twitter.flockdb.conversions.Numeric._
import com.twitter.flockdb.jobs.single.Single
import com.twitter.flockdb.jobs.multi.Multi


// Legacy parsers for old format jobs without state
// XXX: remove once we're off of the old format, or factor out common code with above.

object LegacySingleJobParser {
  def Add(forwardingManager: ForwardingManager, uuidGenerator: UuidGenerator) = {
    new LegacySingleJobParser(forwardingManager, uuidGenerator, State.Normal)
  }

  def Negate(forwardingManager: ForwardingManager, uuidGenerator: UuidGenerator) = {
    new LegacySingleJobParser(forwardingManager, uuidGenerator, State.Negative)
  }

  def Archive(forwardingManager: ForwardingManager, uuidGenerator: UuidGenerator) = {
    new LegacySingleJobParser(forwardingManager, uuidGenerator, State.Archived)
  }

  def Remove(forwardingManager: ForwardingManager, uuidGenerator: UuidGenerator) = {
    new LegacySingleJobParser(forwardingManager, uuidGenerator, State.Removed)
  }
}

object LegacyMultiJobParser {
  def Archive(
    forwardingManager: ForwardingManager,
    scheduler: PrioritizingJobScheduler,
    aggregateJobPageSize: Int
  ) = {
    new LegacyMultiJobParser(forwardingManager, scheduler, aggregateJobPageSize, State.Archived)
  }

  def Unarchive(
    forwardingManager: ForwardingManager,
    scheduler: PrioritizingJobScheduler,
    aggregateJobPageSize: Int
  ) = {
    new LegacyMultiJobParser(forwardingManager, scheduler, aggregateJobPageSize, State.Normal)
  }

  def RemoveAll(
    forwardingManager: ForwardingManager,
    scheduler: PrioritizingJobScheduler,
    aggregateJobPageSize: Int
  ) = {
    new LegacyMultiJobParser(forwardingManager, scheduler, aggregateJobPageSize, State.Removed)
  }

  def Negate(
    forwardingManager: ForwardingManager,
    scheduler: PrioritizingJobScheduler,
    aggregateJobPageSize: Int
  ) = {
    new LegacyMultiJobParser(forwardingManager, scheduler, aggregateJobPageSize, State.Negative)
  }
}

class LegacySingleJobParser(
  forwardingManager: ForwardingManager,
  uuidGenerator: UuidGenerator,
  state: State)
extends JsonJobParser {

  def log = Logger.get

  def apply(attributes: Map[String, Any]): JsonJob = {
    val casted = attributes.asInstanceOf[Map[String, AnyVal]]

    new Single(
      casted("source_id").toLong,
      casted("graph_id").toInt,
      casted("destination_id").toLong,
      casted("position").toLong,
      state, // ONLY DIFFERENCE FROM SingleJobParser
      Time.fromSeconds(casted("updated_at").toInt),
      forwardingManager,
      uuidGenerator
    )
  }
}

class LegacyMultiJobParser(
  forwardingManager: ForwardingManager,
  scheduler: PrioritizingJobScheduler,
  aggregateJobPageSize: Int,
  state: State)
extends JsonJobParser {

  def apply(attributes: Map[String, Any]): JsonJob = {
    val casted = attributes.asInstanceOf[Map[String, AnyVal]]

    new Multi(
      casted("source_id").toLong,
      casted("graph_id").toInt,
      Direction(casted("direction").toInt),
      state,
      Time.fromSeconds(casted("updated_at").toInt),
      Priority(casted.get("priority").map(_.toInt).getOrElse(Priority.Low.id)),
      aggregateJobPageSize,
      forwardingManager,
      scheduler
    )
  }
}
