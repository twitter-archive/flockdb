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
package unit

import scala.collection.mutable
import com.twitter.gizzard.scheduler.{JsonJob, PrioritizingJobScheduler}
import com.twitter.gizzard.shards.{ShardInfo, ShardException, LeafRoutingNode}
import com.twitter.util.Time
import com.twitter.util.TimeConversions._
import org.specs.mock.{ClassMocker, JMocker}
import com.twitter.flockdb
import flockdb.Direction._
import flockdb.State._
import shards.{Shard, SqlShard, ReadWriteShardAdapter, OptimisticLockException}
import jobs.multi.{Archive, RemoveAll, Unarchive}
import jobs.single.{Add, Remove, Archive, NodePair}

class JobSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  val FOLLOWS = 1

  val bob = 1L
  val mary = 23L
  val carl = 42L
  val jane = 56L
  val darcy = 62L

  val uuidGenerator     = IdentityUuidGenerator
  val forwardingManager = mock[ForwardingManager]
  val mocks             = (0 to 3) map { _ => mock[Shard] }

  // allow the readwrite shard adapter to implement optimistically
  val shards            = mocks map { m => new ReadWriteShardAdapter(LeafRoutingNode(m)) }
  val scheduler         = mock[PrioritizingJobScheduler]

  def test(desc: String, jobState: State, bobBefore: State, maryBefore: State, bobAfter: State, maryAfter: State, applied: State, f: jobs.single.Single => Unit) = {
    desc in {
      Time.withCurrentTimeFrozen { time =>
        val job = jobState match {
          case Normal => Add(bob, FOLLOWS, mary, 1, Time.now, forwardingManager, uuidGenerator)
          case Removed => Remove(bob, FOLLOWS, mary, 1, Time.now, forwardingManager, uuidGenerator)
          case Archived => jobs.single.Archive(bob, FOLLOWS, mary, 1, Time.now, forwardingManager, uuidGenerator)
        }
        expect {
          allowing(forwardingManager).find(bob, FOLLOWS, Forward) willReturn shards(0)
          allowing(forwardingManager).find(mary, FOLLOWS, Backward) willReturn shards(1)

          // Before
          one(mocks(0)).getMetadata(bob) willReturn Some(new Metadata(bob, bobBefore, 1, Time.now - 1.second))
          one(mocks(1)).getMetadata(mary) willReturn Some(new Metadata(mary, maryBefore, 1, Time.now - 1.second))

          // After
          allowing(mocks(0)).getMetadata(bob) willReturn Some(new Metadata(mary, bobAfter, 1, Time.now))
          allowing(mocks(1)).getMetadata(mary) willReturn Some(new Metadata(mary, maryAfter, 1, Time.now))

          // Results
          applied match {
            case Normal => {
              one(mocks(0)).add(bob, mary, 1, Time.now)
              one(mocks(1)).add(mary, bob, 1, Time.now)
            }
            case Archived => {
              one(mocks(0)).archive(bob, mary, 1, Time.now)
              one(mocks(1)).archive(mary, bob, 1, Time.now)
            }
            case Removed => {
              one(mocks(0)).remove(bob, mary, 1, Time.now)
              one(mocks(1)).remove(mary, bob, 1, Time.now)
            }
          }
        }
        f(job)
      }
    }
  }

  "Add" should {
    //                         Input   Before            After             Resulting
    //                         Job     Bob     Mary      Bob     Mary      Job
    test("normal add",         Normal, Normal, Normal,   Normal, Normal,   Normal, _.apply)
    test("lost lock add",      Normal, Normal, Normal,   Normal, Archived, Normal, _.apply must throwA[OptimisticLockException])
    test("when bob archived",  Normal, Archived, Normal, Archived, Normal, Archived, _.apply)
    test("when mary archived", Normal, Normal, Archived, Normal, Archived, Archived, _.apply)

    "toJson" in {
      Time.withCurrentTimeFrozen { time =>
        val job = Add(bob, FOLLOWS, mary, 1, Time.now, forwardingManager, uuidGenerator)
        val json = job.toJson
        json mustMatch "Add"
        json mustMatch "\"source_id\":" + bob
        json mustMatch "\"graph_id\":" + FOLLOWS
        json mustMatch "\"destination_id\":" + mary
        json mustMatch "\"updated_at\":" + Time.now.inSeconds
      }
    }
  }

  "Remove" should {
    //                         Input    Before            After             Resulting
    //                         Job      Bob     Mary      Bob     Mary      Job
    test("normal remove",      Removed, Normal, Normal,   Normal, Normal,   Removed, _.apply)

    "toJson" in {
      Time.withCurrentTimeFrozen { time =>
        val job = new Remove(bob, FOLLOWS, mary, 1, Time.now, forwardingManager, uuidGenerator)
        val json = job.toJson
        json mustMatch "Remove"
        json mustMatch "\"source_id\":" + bob
        json mustMatch "\"graph_id\":" + FOLLOWS
        json mustMatch "\"destination_id\":" + mary
        json mustMatch "\"updated_at\":" + Time.now.inSeconds
      }
    }
  }

  "Archive" should {
    //                          Input     Before             After             Resulting
    //                          Job       Bob     Mary       Bob     Mary      Job
    test("normal archive",      Archived, Normal, Normal,    Normal, Normal,   Archived, _.apply)
    test("archive removed",     Archived, Normal, Removed,   Normal, Removed,  Removed, _.apply)
    test("archive removed",     Archived, Removed, Normal,   Removed, Normal,  Removed, _.apply)


    "toJson" in {
      Time.withCurrentTimeFrozen { time =>
        val job = new jobs.single.Archive(bob, FOLLOWS, mary, 1, Time.now, forwardingManager, uuidGenerator)
        val json = job.toJson
        json mustMatch "Archive"
        json mustMatch "\"source_id\":" + bob
        json mustMatch "\"graph_id\":" + FOLLOWS
        json mustMatch "\"destination_id\":" + mary
        json mustMatch "\"updated_at\":" + Time.now.inSeconds
      }
    }
  }

  "Archive" should {
    "toJson" in {
      Time.withCurrentTimeFrozen { time =>
        val job = new jobs.multi.Archive(bob, FOLLOWS, Direction.Forward, Time.now, Priority.Low, config.aggregateJobsPageSize, forwardingManager, scheduler)
        val json = job.toJson
        json mustMatch "Archive"
        json mustMatch "\"source_id\":" + bob
        json mustMatch "\"graph_id\":" + FOLLOWS
        json mustMatch "\"updated_at\":" + Time.now.inSeconds
        json mustMatch "\"priority\":" + Priority.Low.id
      }
    }
  }

  "Unarchive" should {
    "toJson" in {
      Time.withCurrentTimeFrozen { time =>
        val job = new Unarchive(bob, FOLLOWS, Direction.Forward, Time.now, Priority.Low, config.aggregateJobsPageSize, forwardingManager, scheduler)
        val json = job.toJson
        json mustMatch "Unarchive"
        json mustMatch "\"source_id\":" + bob
        json mustMatch "\"graph_id\":" + FOLLOWS
        json mustMatch "\"updated_at\":" + Time.now.inSeconds
        json mustMatch "\"priority\":" + Priority.Low.id
      }
    }
  }

  "RemoveAll" should {
    "toJson" in {
      Time.withCurrentTimeFrozen { time =>
        val job = RemoveAll(bob, FOLLOWS, Direction.Backward, Time.now, Priority.Low, config.aggregateJobsPageSize, forwardingManager, scheduler)
        val json = job.toJson
        json mustMatch "RemoveAll"
        json mustMatch "\"source_id\":" + bob
        json mustMatch "\"graph_id\":" + FOLLOWS
        json mustMatch "\"updated_at\":" + Time.now.inSeconds
        json mustMatch "\"priority\":" + Priority.Low.id
      }
    }
  }
}
