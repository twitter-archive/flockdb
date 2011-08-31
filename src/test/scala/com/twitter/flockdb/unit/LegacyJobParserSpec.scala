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

package com.twitter.flockdb.unit

import com.twitter.util.Time
import com.twitter.gizzard.scheduler.JsonCodec
import com.twitter.flockdb.ConfiguredSpecification
import com.twitter.flockdb.{Direction, State, Priority}
import com.twitter.flockdb.jobs.single.Single
import com.twitter.flockdb.jobs.multi.Multi
import com.twitter.flockdb.jobs._


class LegacyJobParserSpec extends ConfiguredSpecification {

  val updatedAt = Time.fromSeconds(1111)
  val codec = new JsonCodec(_ => ())

  codec += ("com.twitter.flockdb.jobs.single.Add".r,      LegacySingleJobParser.Add(null, null))
  codec += ("com.twitter.flockdb.jobs.single.Remove".r,   LegacySingleJobParser.Remove(null, null))
  codec += ("com.twitter.flockdb.jobs.single.Negate".r,   LegacySingleJobParser.Negate(null, null))
  codec += ("com.twitter.flockdb.jobs.single.Archive".r,  LegacySingleJobParser.Archive(null, null))
  codec += ("com.twitter.flockdb.jobs.multi.Archive".r,   LegacyMultiJobParser.Archive(null, null, 500))
  codec += ("com.twitter.flockdb.jobs.multi.Unarchive".r, LegacyMultiJobParser.Unarchive(null, null, 500))
  codec += ("com.twitter.flockdb.jobs.multi.RemoveAll".r, LegacyMultiJobParser.RemoveAll(null, null, 500))
  codec += ("com.twitter.flockdb.jobs.multi.Negate".r,    LegacyMultiJobParser.Negate(null, null, 500))

  "LegacySingleJobParser" should {
    "correctly generate a new style job from an old serialized Add job" in {
      val map = Map(
        "com.twitter.flockdb.jobs.single.Add" -> Map(
          "source_id" -> 22,
          "graph_id" -> 1,
          "destination_id" -> 11,
          "position" -> 1111,
          "updated_at" -> 1111
        )
      )

      codec.inflate(map) mustEqual new Single(22, 1, 11, 1111, State.Normal, updatedAt, null, null)
    }

    "correctly generate a new style job from an old serialized Remove job" in {
      val map = Map(
        "com.twitter.flockdb.jobs.single.Remove" -> Map(
          "source_id" -> 22,
          "graph_id" -> 1,
          "destination_id" -> 11,
          "position" -> 1111,
          "updated_at" -> 1111
        )
      )

      codec.inflate(map) mustEqual new Single(22, 1, 11, 1111, State.Removed, updatedAt, null, null)
    }

    "correctly generate a new style job from an old serialized Negate job" in {
      val map = Map(
        "com.twitter.flockdb.jobs.single.Negate" -> Map(
          "source_id" -> 22,
          "graph_id" -> 1,
          "destination_id" -> 11,
          "position" -> 1111,
          "updated_at" -> 1111
        )
      )

      codec.inflate(map) mustEqual new Single(22, 1, 11, 1111, State.Negative, updatedAt, null, null)
    }

    "correctly generate a new style job from an old serialized Archive job" in {
      val map = Map(
        "com.twitter.flockdb.jobs.single.Archive" -> Map(
          "source_id" -> 22,
          "graph_id" -> 1,
          "destination_id" -> 11,
          "position" -> 1111,
          "updated_at" -> 1111
        )
      )

      codec.inflate(map) mustEqual new Single(22, 1, 11, 1111, State.Archived, updatedAt, null, null)
    }
  }

  "LegacyMultiJobParser" should {
    "correctly generate a new style job from an old serialized Archive job" in {
      val map = Map(
        "com.twitter.flockdb.jobs.multi.Archive" -> Map(
          "source_id" -> 22,
          "graph_id" -> 1,
          "direction" -> 0,
          "updated_at" -> 1111,
          "priority" -> 1
        )
      )

      val job = new Multi(22, 1, Direction.Forward, State.Archived, updatedAt, Priority.Low, 500, null, null)

      codec.inflate(map) mustEqual job
    }

    "correctly generate a new style job from an old serialized Unarchive job" in {
      val map = Map(
        "com.twitter.flockdb.jobs.multi.Unarchive" -> Map(
          "source_id" -> 22,
          "graph_id" -> 1,
          "direction" -> 0,
          "updated_at" -> 1111,
          "priority" -> 1
        )
      )

      val job = new Multi(22, 1, Direction.Forward, State.Normal, updatedAt, Priority.Low, 500, null, null)

      codec.inflate(map) mustEqual job
    }

    "correctly generate a new style job from an old serialized RemoveAll job" in {
      val map = Map(
        "com.twitter.flockdb.jobs.multi.RemoveAll" -> Map(
          "source_id" -> 22,
          "graph_id" -> 1,
          "direction" -> 0,
          "updated_at" -> 1111,
          "priority" -> 1
        )
      )

      val job = new Multi(22, 1, Direction.Forward, State.Removed, updatedAt, Priority.Low, 500, null, null)

      codec.inflate(map) mustEqual job
    }

    "correctly generate a new style job from an old serialized Negate job" in {
      val map = Map(
        "com.twitter.flockdb.jobs.multi.Negate" -> Map(
          "source_id" -> 22,
          "graph_id" -> 1,
          "direction" -> 0,
          "updated_at" -> 1111,
          "priority" -> 1
        )
      )

      val job = new Multi(22, 1, Direction.Forward, State.Negative, updatedAt, Priority.Low, 500, null, null)

      codec.inflate(map) mustEqual job
    }
  }
}
