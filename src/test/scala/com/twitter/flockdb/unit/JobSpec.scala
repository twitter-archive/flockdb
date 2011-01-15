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

import scala.collection.mutable
import com.twitter.gizzard.scheduler.{JsonJob, PrioritizingJobScheduler}
import com.twitter.gizzard.shards.{ShardInfo, ShardException}
import com.twitter.util.Time
import flockdb.Direction._
import flockdb.State._
import flockdb.shards.ReadWriteShardAdapter
import flockdb.shards.OptimisticLockException
import gizzard.shards.{ReadWriteShard, FanoutResults}
import com.twitter.util.TimeConversions._
import org.specs.mock.{ClassMocker, JMocker}
import jobs.multi.{Archive, RemoveAll, Unarchive}
import jobs.single.{Add, Remove, Archive, NodePair}
import shards.{Shard, SqlShard}
import flockdb.Metadata
import thrift.Edge

class FakeLockingShard(shard: Shard) extends SqlShard(null, new ShardInfo("a", "b", "c"), 1, Nil, 0) {
  override def withLock[A](sourceId: Long)(f: (Shard, Metadata) => A) = f(shard, shard.getMetadata(sourceId).get) // jMock is not up to the task
}

class SimpleAdapter(shard: Shard) extends ReadWriteShardAdapter(new IdentityShard[Shard](shard))

class IdentityShard[ConcreteShard <: Shard](shard: ConcreteShard)
  extends ReadWriteShard[ConcreteShard] {
  val children = Seq(shard)
  val weight = 1
  def shardInfo = throw new UnsupportedOperationException()

  def readAllOperation[A](method: (ConcreteShard => A)) = FanoutResults(method, shard)
  def readOperation[A](method: (ConcreteShard => A)) = method(shard)

  def writeOperation[A](method: (ConcreteShard => A)) = method(shard)

  def rebuildableReadOperation[A](method: (ConcreteShard => Option[A]))(rebuild: (ConcreteShard, ConcreteShard) => Unit) =
    method(shard)
}

class JobSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  val FOLLOWS = 1

  val bob = 1L
  val mary = 23L
  val carl = 42L
  val jane = 56L
  val darcy = 62L

  val uuidGenerator = IdentityUuidGenerator
  var forwardingManager: ForwardingManager = null
  var shard1: Shard = null
  var shard2: Shard = null
  var shard3: Shard = null
  var shard4: Shard = null
  var shard1Mock: Shard = null
  var shard2Mock: Shard = null
  val scheduler = mock[PrioritizingJobScheduler[JsonJob]]

  def before() {
    doBefore {
      forwardingManager = mock[ForwardingManager]
      shard1Mock = mock[Shard]
      shard2Mock = mock[Shard]
      shard1 = new SimpleAdapter(shard1Mock)
      shard2 = new SimpleAdapter(shard2Mock)
    }
  }

  def test(desc: String, jobState: State, bobBefore: State, maryBefore: State, bobAfter: State, maryAfter: State, applied: State, f: jobs.single.Single => Unit) = {
    desc in {
      Time.withCurrentTimeFrozen { time =>
        val job = jobState match {
          case Normal => Add(bob, FOLLOWS, mary, 1, Time.now, forwardingManager, uuidGenerator)
          case Removed => Remove(bob, FOLLOWS, mary, 1, Time.now, forwardingManager, uuidGenerator)
          case Archived => jobs.single.Archive(bob, FOLLOWS, mary, 1, Time.now, forwardingManager, uuidGenerator)
        }
        expect {
          allowing(forwardingManager).find(bob, FOLLOWS, Forward) willReturn shard1
          allowing(forwardingManager).find(mary, FOLLOWS, Backward) willReturn shard2

          // Before
          one(shard1Mock).getMetadata(bob) willReturn Some(Metadata(bob, bobBefore, 1, Time.now - 1.second))
          one(shard2Mock).getMetadata(mary) willReturn Some(Metadata(mary, maryBefore, 1, Time.now - 1.second))

          // After
          allowing(shard1Mock).getMetadata(bob) willReturn Some(Metadata(mary, bobAfter, 1, Time.now))
          allowing(shard2Mock).getMetadata(mary) willReturn Some(Metadata(mary, maryAfter, 1, Time.now))

          // Results
          applied match {
            case Normal => {
              one(shard1Mock).add(bob, mary, 1, Time.now)
              one(shard2Mock).add(mary, bob, 1, Time.now)
            }
            case Archived => {
              one(shard1Mock).archive(bob, mary, 1, Time.now)
              one(shard2Mock).archive(mary, bob, 1, Time.now)
            }
            case Removed => {
              one(shard1Mock).remove(bob, mary, 1, Time.now)
              one(shard2Mock).remove(mary, bob, 1, Time.now)
            }
          }
        }
        f(job)
      }
    }
  }

  "Add" should {
    before()
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
    before()

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
    before()

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
    doBefore {
      forwardingManager = mock[ForwardingManager]
      shard1 = mock[Shard]
      shard2 = mock[Shard]
      shard3 = mock[Shard]
      shard4 = mock[Shard]
    }

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
    doBefore {
      forwardingManager = mock[ForwardingManager]
      shard1 = mock[Shard]
      shard2 = mock[Shard]
      shard3 = mock[Shard]
      shard4 = mock[Shard]
    }

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
    doBefore {
      forwardingManager = mock[ForwardingManager]
      shard1 = mock[Shard]
      shard2 = mock[Shard]
      shard3 = mock[Shard]
      shard4 = mock[Shard]
    }

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