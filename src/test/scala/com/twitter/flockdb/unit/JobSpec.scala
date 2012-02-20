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

import com.twitter.gizzard.scheduler.{JsonJob, PrioritizingJobScheduler}
import com.twitter.gizzard.shards.ShardInfo
import com.twitter.util.Time
import com.twitter.util.TimeConversions._
import org.specs.mock.{ClassMocker, JMocker}
import jobs.multi.{Archive, RemoveAll, Unarchive}
import jobs.single.{Add, Remove}
import shards.{Shard, SqlShard}
import flockdb.Metadata
import thrift.Edge


class FakeLockingShard(shard: Shard) extends SqlShard(null, new ShardInfo("a", "b", "c"), 1, Nil, 0) {
  override def withLock[A](sourceId: Long, updatedAt: Time)(f: (Shard, Metadata) => A) = f(shard, shard.getMetadata(sourceId).get) // jMock is not up to the task
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
  var lockingShard1: Shard = null
  var lockingShard2: Shard = null
  val scheduler = mock[PrioritizingJobScheduler[JsonJob]]

  "Add" should {
    doBefore {
      forwardingManager = mock[ForwardingManager]
      shard1 = mock[Shard]
      shard2 = mock[Shard]
      lockingShard1 = new FakeLockingShard(shard1)
      lockingShard2 = new FakeLockingShard(shard2)
    }

    "apply" in {
      "when the add takes effect" >> {
        Time.withCurrentTimeFrozen { time =>
          val job = Add(bob, FOLLOWS, mary, 1, Time.now, forwardingManager, uuidGenerator)

          expect {
            one(forwardingManager).find(bob, FOLLOWS, Direction.Forward) willReturn lockingShard1
            one(forwardingManager).find(mary, FOLLOWS, Direction.Backward) willReturn lockingShard2
            one(shard1).getMetadata(bob) willReturn Some(Metadata(bob, State.Normal, 1, Time.now))
            one(shard2).getMetadata(mary) willReturn Some(Metadata(mary, State.Normal, 1, Time.now))
            one(shard1).add(bob, mary, 1, Time.now)
            one(shard2).add(mary, bob, 1, Time.now)
          }

          job.apply()
        }
      }

      "when the add does not take effect" >> {
        "when the forward direction causes it to not take effect" >> {
          Time.withCurrentTimeFrozen { time =>
            val job = Add(bob, FOLLOWS, mary, 1, Time.now, forwardingManager, uuidGenerator)

            expect {
              one(forwardingManager).find(bob, FOLLOWS, Direction.Forward) willReturn lockingShard1
              one(forwardingManager).find(mary, FOLLOWS, Direction.Backward) willReturn lockingShard2
              one(shard1).getMetadata(bob) willReturn Some(Metadata(bob, State.Archived, 1, Time.now))
              one(shard2).getMetadata(mary) willReturn Some(Metadata(mary, State.Normal, 1, Time.now))
              one(shard1).archive(bob, mary, 1, Time.now)
              one(shard2).archive(mary, bob, 1, Time.now)
            }

            job.apply()
          }
        }

        "when the backward direction causes it to not take effect" >> {
          Time.withCurrentTimeFrozen { time =>
            val job = Add(bob, FOLLOWS, mary, 1, Time.now, forwardingManager, uuidGenerator)

            expect {
              one(forwardingManager).find(bob, FOLLOWS, Direction.Forward) willReturn lockingShard1
              one(forwardingManager).find(mary, FOLLOWS, Direction.Backward) willReturn lockingShard2
              one(shard1).getMetadata(bob) willReturn Some(Metadata(bob, State.Normal, 1, Time.now))
              one(shard2).getMetadata(mary) willReturn Some(Metadata(mary, State.Archived, 1, Time.now))
              one(shard1).archive(bob, mary, 1, Time.now)
              one(shard2).archive(mary, bob, 1, Time.now)
            }

            job.apply()
          }
        }
      }
    }

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
    doBefore {
      forwardingManager = mock[ForwardingManager]
      shard1 = mock[Shard]
      shard2 = mock[Shard]
      lockingShard1 = new FakeLockingShard(shard1)
      lockingShard2 = new FakeLockingShard(shard2)
    }

    "apply" in {
      "when the remove takes effect" >> {
        Time.withCurrentTimeFrozen { time =>
          val job = new Remove(bob, FOLLOWS, mary, 1, Time.now, forwardingManager, uuidGenerator)

          expect {
            one(forwardingManager).find(bob, FOLLOWS, Direction.Forward) willReturn lockingShard1
            one(forwardingManager).find(mary, FOLLOWS, Direction.Backward) willReturn lockingShard2
            one(shard1).getMetadata(bob) willReturn Some(Metadata(bob, State.Normal, 1, Time.now))
            one(shard2).getMetadata(mary) willReturn Some(Metadata(mary, State.Normal, 1, Time.now))
            one(shard1).remove(bob, mary, 1, Time.now)
            one(shard2).remove(mary, bob, 1, Time.now)
          }

          job.apply()
        }
      }
    }

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
    doBefore {
      forwardingManager = mock[ForwardingManager]
      shard1 = mock[Shard]
      shard2 = mock[Shard]
      lockingShard1 = new FakeLockingShard(shard1)
      lockingShard2 = new FakeLockingShard(shard2)
    }

    "apply" in {
      "when the archive takes effect" >> {
        Time.withCurrentTimeFrozen { time =>
          val job = new jobs.single.Archive(bob, FOLLOWS, mary, 1, Time.now, forwardingManager, uuidGenerator)

          expect {
            one(forwardingManager).find(bob, FOLLOWS, Direction.Forward) willReturn lockingShard1
            one(forwardingManager).find(mary, FOLLOWS, Direction.Backward) willReturn lockingShard2
            one(shard1).getMetadata(bob) willReturn Some(Metadata(bob, State.Normal, 1, Time.now))
            one(shard2).getMetadata(mary) willReturn Some(Metadata(mary, State.Normal, 1, Time.now))
            one(shard1).archive(bob, mary, 1, Time.now)
            one(shard2).archive(mary, bob, 1, Time.now)
          }

          job.apply()
        }
      }

      "when the archive does not take effect" >> {
        "when the forward direction causes it to not take effect" >> {
          Time.withCurrentTimeFrozen { time =>
            val job = new jobs.single.Archive(bob, FOLLOWS, mary, 1, Time.now, forwardingManager, uuidGenerator)

            expect {
              one(forwardingManager).find(bob, FOLLOWS, Direction.Forward) willReturn lockingShard1
              one(forwardingManager).find(mary, FOLLOWS, Direction.Backward) willReturn lockingShard2
              one(shard1).getMetadata(bob) willReturn Some(Metadata(bob, State.Removed, 1, Time.now))
              one(shard2).getMetadata(mary) willReturn Some(Metadata(mary, State.Normal, 1, Time.now))
              one(shard1).remove(bob, mary, 1, Time.now)
              one(shard2).remove(mary, bob, 1, Time.now)
            }

            job.apply()
          }
        }

        "when the backward direction causes it to not take effect" >> {
          Time.withCurrentTimeFrozen { time =>
            val job = new jobs.single.Archive(bob, FOLLOWS, mary, 1, Time.now, forwardingManager, uuidGenerator)

            expect {
              one(forwardingManager).find(bob, FOLLOWS, Direction.Forward) willReturn lockingShard1
              one(forwardingManager).find(mary, FOLLOWS, Direction.Backward) willReturn lockingShard2
              one(shard1).getMetadata(bob) willReturn Some(Metadata(bob, State.Normal, 1, Time.now))
              one(shard2).getMetadata(mary) willReturn Some(Metadata(mary, State.Removed, 1, Time.now))
              one(shard1).remove(bob, mary, 1, Time.now)
              one(shard2).remove(mary, bob, 1, Time.now)
            }

            job.apply()
          }
        }
      }
    }

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
        val job = new Archive(bob, FOLLOWS, Direction.Forward, Time.now, Priority.Low, config.aggregateJobsPageSize, forwardingManager, scheduler)
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
