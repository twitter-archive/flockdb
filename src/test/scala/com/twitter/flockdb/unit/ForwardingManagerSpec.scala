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

import com.twitter.gizzard.nameserver.NameServer
import com.twitter.xrayspecs.Time
import com.twitter.xrayspecs.TimeConversions._
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}
import shards.{Metadata, Shard}

class ForwardingManagerSpec extends Specification with JMocker with ClassMocker {
  def work(forwardShard: Shard, backwardShard: Shard, nodePair: NodePair, state: State) {
    // nothing.
  }

  "ForwardingManager" should {
    val nameServer = mock[NameServer[Shard]]
    val forwardingManager = new ForwardingManager(nameServer)
    val shard1 = mock[Shard]
    val shard2 = mock[Shard]
    val nodePair1 = NodePair(1L, 2L)
    val nodePair2 = NodePair(3L, 4L)
    val nodePair3 = NodePair(3L, 13L)

    doBefore {
      Time.freeze()
    }

    def returnShards() {
      allowing(nameServer).findCurrentForwarding(0, nodePair1.sourceId) willReturn shard1
      allowing(nameServer).findCurrentForwarding(0, nodePair1.destinationId) willReturn shard2
      allowing(nameServer).findCurrentForwarding(0, nodePair2.sourceId) willReturn shard1
      allowing(nameServer).findCurrentForwarding(0, nodePair2.destinationId) willReturn shard2
      allowing(nameServer).findCurrentForwarding(0, nodePair3.destinationId) willReturn shard2
    }

    def returnStates(nodePair: NodePair, sourceState: State, destinationState: State) {
      one(shard1).getMetadata(nodePair.sourceId) willReturn Some(Metadata(nodePair.sourceId, sourceState, 0, Time.now))
      one(shard2).getMetadata(nodePair.destinationId) willReturn Some(Metadata(nodePair.destinationId, destinationState, 0, Time.now))
    }

    "withOptimisticLocks" in {
      "basic single pair" in {
        "success" in {
          expect {
            returnShards()
            returnStates(nodePair1, State.Normal, State.Normal)
            returnStates(nodePair1, State.Normal, State.Normal)
          }

          forwardingManager.withOptimisticLocks(0, List(nodePair1))(work) mustEqual Nil
        }

        "failure" in {
          expect {
            returnShards()
            returnStates(nodePair1, State.Normal, State.Archived)
            returnStates(nodePair1, State.Normal, State.Normal)
          }

          forwardingManager.withOptimisticLocks(0, List(nodePair1))(work) mustEqual List(nodePair1)
        }
      }

      "several pairs" in {
        "only ask once for each id" in {
          expect {
            returnShards()
            returnStates(nodePair1, State.Normal, State.Normal)
            returnStates(nodePair2, State.Normal, State.Normal)
            returnStates(nodePair1, State.Normal, State.Normal)
            returnStates(nodePair2, State.Normal, State.Normal)
            one(shard2).getMetadata(nodePair3.destinationId) willReturn Some(Metadata(nodePair3.destinationId, State.Normal, 0, Time.now))
            one(shard2).getMetadata(nodePair3.destinationId) willReturn Some(Metadata(nodePair3.destinationId, State.Normal, 0, Time.now))
          }

          forwardingManager.withOptimisticLocks(0, List(nodePair1, nodePair2, nodePair3))(work) mustEqual Nil
        }

        "more than one failure" in {
          expect {
            returnShards()
            returnStates(nodePair1, State.Normal, State.Normal)
            returnStates(nodePair2, State.Normal, State.Normal)
            returnStates(nodePair1, State.Normal, State.Removed)
            returnStates(nodePair2, State.Normal, State.Normal)
            one(shard2).getMetadata(nodePair3.destinationId) willReturn Some(Metadata(nodePair3.destinationId, State.Normal, 0, Time.now))
            one(shard2).getMetadata(nodePair3.destinationId) willReturn Some(Metadata(nodePair3.destinationId, State.Archived, 0, Time.now))
          }

          forwardingManager.withOptimisticLocks(0, List(nodePair1, nodePair2, nodePair3))(work) mustEqual List(nodePair1, nodePair3)
        }
      }
    }
  }
}
