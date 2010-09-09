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
import com.twitter.gizzard.shards.ShardException
import com.twitter.xrayspecs.Time
import com.twitter.xrayspecs.TimeConversions._
import org.specs.mock.{ClassMocker, JMocker}
import jobs.single.{Add, Remove}
import shards.{BlackHoleShard, Shard, Metadata}
import thrift.Edge

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

  val wrappedForwardingManager = new ForwardingManager(null) {
    val shardMap = new mutable.HashMap[Long, Shard]
    val stateMap = new mutable.HashMap[Long, State]
    val lostLocks = new mutable.ListBuffer[NodePair]

    override def find(sourceId: Long, graphId: Int, direction: Direction) = forwardingManager.find(sourceId, graphId, direction)

    override def withOptimisticLocks(graphId: Int, nodePairs: Seq[NodePair])(f: (Shard, Shard, NodePair, State) => Unit) = {
      nodePairs.foreach { nodePair =>
        val forwardShard = shardMap(nodePair.sourceId)
        val backwardShard = shardMap(nodePair.destinationId)
        val forwardState = stateMap(nodePair.sourceId)
        val backwardState = stateMap(nodePair.destinationId)
        f(shard1, shard2, nodePair, forwardState max backwardState)
      }
      lostLocks.toList
    }
  }

  "Add" should {
    doBefore {
      Time.freeze
      forwardingManager = mock[ForwardingManager]
      shard1 = mock[Shard]
      shard2 = mock[Shard]
      wrappedForwardingManager.shardMap.clear()
      wrappedForwardingManager.stateMap.clear()
      wrappedForwardingManager.lostLocks.clear()
    }

    "apply" in {
      "when the add takes effect" in {
        val job = Add(bob, FOLLOWS, mary, 1, Time.now)

        expect {
          wrappedForwardingManager.shardMap(bob) = shard1
          wrappedForwardingManager.shardMap(mary) = shard2
          wrappedForwardingManager.stateMap(bob) = State.Normal
          wrappedForwardingManager.stateMap(mary) = State.Normal
          one(shard1).add(bob, mary, 1, Time.now)
          one(shard2).add(mary, bob, 1, Time.now)
        }

        job.apply((wrappedForwardingManager, uuidGenerator))
      }

      "when the add does not take effect" >> {
        "when the forward direction causes it to not take effect" >> {
          val job = Add(bob, FOLLOWS, mary, 1, Time.now)

          expect {
            wrappedForwardingManager.shardMap(bob) = shard1
            wrappedForwardingManager.shardMap(mary) = shard2
            wrappedForwardingManager.stateMap(bob) = State.Archived
            wrappedForwardingManager.stateMap(mary) = State.Normal
            one(shard1).archive(bob, mary, 1, Time.now)
            one(shard2).archive(mary, bob, 1, Time.now)
          }

          job.apply((wrappedForwardingManager, uuidGenerator))
        }

        "when the backward direction causes it to not take effect" >> {
          val job = Add(bob, FOLLOWS, mary, 1, Time.now)

          expect {
            wrappedForwardingManager.shardMap(bob) = shard1
            wrappedForwardingManager.shardMap(mary) = shard2
            wrappedForwardingManager.stateMap(bob) = State.Normal
            wrappedForwardingManager.stateMap(mary) = State.Archived
            one(shard1).archive(bob, mary, 1, Time.now)
            one(shard2).archive(mary, bob, 1, Time.now)
          }

          job.apply((wrappedForwardingManager, uuidGenerator))
        }
      }

      "if node metadata changes during the operation" in {
        val job = Add(bob, FOLLOWS, mary, 1, Time.now)

        expect {
          wrappedForwardingManager.shardMap(bob) = shard1
          wrappedForwardingManager.shardMap(mary) = shard2
          wrappedForwardingManager.stateMap(bob) = State.Normal
          wrappedForwardingManager.stateMap(mary) = State.Archived
          wrappedForwardingManager.lostLocks += NodePair(bob, mary)
          one(shard1).archive(bob, mary, 1, Time.now)
          one(shard2).archive(mary, bob, 1, Time.now)
        }

        job.apply((wrappedForwardingManager, uuidGenerator)) must throwA[ShardException]
      }
    }

    "toJson" in {
      val job = Add(bob, FOLLOWS, mary, 1, Time.now)
      val json = job.toJson
      json mustMatch "Add"
      json mustMatch "\"source_id\":" + bob
      json mustMatch "\"graph_id\":" + FOLLOWS
      json mustMatch "\"destination_id\":" + mary
      json mustMatch "\"updated_at\":" + Time.now
    }
  }

  "Remove" should {
    doBefore {
      forwardingManager = mock[ForwardingManager]
      shard1 = mock[Shard]
      shard2 = mock[Shard]
    }

    "apply" in {
      "when the remove takes effect" >> {
        val job = new Remove(bob, FOLLOWS, mary, 1, Time.now)

        expect {
          wrappedForwardingManager.shardMap(bob) = shard1
          wrappedForwardingManager.shardMap(mary) = shard2
          wrappedForwardingManager.stateMap(bob) = State.Normal
          wrappedForwardingManager.stateMap(mary) = State.Normal
          one(shard1).remove(bob, mary, 1, Time.now)
          one(shard2).remove(mary, bob, 1, Time.now)
        }

        job.apply((wrappedForwardingManager, uuidGenerator))
      }
    }

    "toJson" in {
      val job = new Remove(bob, FOLLOWS, mary, 1, Time.now)
      val json = job.toJson
      json mustMatch "Remove"
      json mustMatch "\"source_id\":" + bob
      json mustMatch "\"graph_id\":" + FOLLOWS
      json mustMatch "\"destination_id\":" + mary
      json mustMatch "\"updated_at\":" + Time.now
    }
  }

  "Archive" should {
    doBefore {
      forwardingManager = mock[ForwardingManager]
      shard1 = mock[Shard]
      shard2 = mock[Shard]
    }

    "apply" in {
      "when the archive takes effect" >> {
        val time = System.currentTimeMillis
        val job = new jobs.single.Archive(bob, FOLLOWS, mary, 1, Time.now)

        expect {
          wrappedForwardingManager.shardMap(bob) = shard1
          wrappedForwardingManager.shardMap(mary) = shard2
          wrappedForwardingManager.stateMap(bob) = State.Normal
          wrappedForwardingManager.stateMap(mary) = State.Normal
          one(shard1).archive(bob, mary, 1, Time.now)
          one(shard2).archive(mary, bob, 1, Time.now)
        }

        job.apply((wrappedForwardingManager, uuidGenerator))
      }

      "when the archive does not take effect" >> {
        "when the forward direction causes it to not take effect" >> {
          val job = new jobs.single.Archive(bob, FOLLOWS, mary, 1, Time.now)

          expect {
            wrappedForwardingManager.shardMap(bob) = shard1
            wrappedForwardingManager.shardMap(mary) = shard2
            wrappedForwardingManager.stateMap(bob) = State.Removed
            wrappedForwardingManager.stateMap(mary) = State.Normal
            one(shard1).remove(bob, mary, 1, Time.now)
            one(shard2).remove(mary, bob, 1, Time.now)
          }

          job.apply((wrappedForwardingManager, uuidGenerator))
        }

        "when the backward direction causes it to not take effect" >> {
          val job = new jobs.single.Archive(bob, FOLLOWS, mary, 1, Time.now)

          expect {
            wrappedForwardingManager.shardMap(bob) = shard1
            wrappedForwardingManager.shardMap(mary) = shard2
            wrappedForwardingManager.stateMap(bob) = State.Normal
            wrappedForwardingManager.stateMap(mary) = State.Removed
            one(shard1).remove(bob, mary, 1, Time.now)
            one(shard2).remove(mary, bob, 1, Time.now)
          }

          job.apply((wrappedForwardingManager, uuidGenerator))
        }
      }
    }

    "toJson" in {
      val job = new jobs.single.Archive(bob, FOLLOWS, mary, 1, Time.now)
      val json = job.toJson
      json mustMatch "Archive"
      json mustMatch "\"source_id\":" + bob
      json mustMatch "\"graph_id\":" + FOLLOWS
      json mustMatch "\"destination_id\":" + mary
      json mustMatch "\"updated_at\":" + Time.now
    }
  }

  "Archive multi" should {
    doBefore {
      forwardingManager = mock[ForwardingManager]
      shard1 = mock[Shard]
      shard2 = mock[Shard]
      shard3 = mock[Shard]
      shard4 = mock[Shard]
    }

    "toJson" in {
      val job = new jobs.multi.Archive(bob, FOLLOWS, Direction.Forward, Time.now, Priority.Low)
      val json = job.toJson
      json mustMatch "Archive"
      json mustMatch "\"source_id\":" + bob
      json mustMatch "\"graph_id\":" + FOLLOWS
      json mustMatch "\"updated_at\":" + Time.now
      json mustMatch "\"priority\":" + Priority.Low.id
    }
  }

  "Unarchive multi" should {
    doBefore {
      forwardingManager = mock[ForwardingManager]
      shard1 = mock[Shard]
      shard2 = mock[Shard]
      shard3 = mock[Shard]
      shard4 = mock[Shard]
    }

    "toJson" in {
      val job = new jobs.multi.Unarchive(bob, FOLLOWS, Direction.Forward, Time.now, Priority.Low)
      val json = job.toJson
      json mustMatch "Unarchive"
      json mustMatch "\"source_id\":" + bob
      json mustMatch "\"graph_id\":" + FOLLOWS
      json mustMatch "\"updated_at\":" + Time.now
      json mustMatch "\"priority\":" + Priority.Low.id
    }
  }

  "RemoveAll multi" should {
    doBefore {
      forwardingManager = mock[ForwardingManager]
      shard1 = mock[Shard]
      shard2 = mock[Shard]
      shard3 = mock[Shard]
      shard4 = mock[Shard]
    }

    "toJson" in {
      val job = jobs.multi.RemoveAll(bob, FOLLOWS, Direction.Backward, Time.now, Priority.Low)
      val json = job.toJson
      json mustMatch "RemoveAll"
      json mustMatch "\"source_id\":" + bob
      json mustMatch "\"graph_id\":" + FOLLOWS
      json mustMatch "\"updated_at\":" + Time.now
      json mustMatch "\"priority\":" + Priority.Low.id
    }
  }
}
