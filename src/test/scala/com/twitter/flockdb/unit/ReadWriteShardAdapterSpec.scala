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

import com.twitter.gizzard.shards.{ReadWriteShard, ReplicatingShard, ShardInfo}
import com.twitter.gizzard.test.FakeReadWriteShard
import com.twitter.xrayspecs.Time
import com.twitter.xrayspecs.TimeConversions._
import org.specs.mock.{ClassMocker, JMocker}
import shards.{ReadWriteShardAdapter, Shard, Metadata}
import thrift.Edge


object ReadWriteShardAdapterSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  var shard1: Shard = null
  var shard2: Shard = null
  var shard3: Shard = null

  "ReadWriteShardAdapter" should {
    doBefore {
      shard1 = mock[Shard]
      shard2 = mock[Shard]
      shard3 = mock[Shard]
      Time.freeze
    }

    "call withLock only on the primary child" in {
      val sourceId = 23
      val metadata = mock[Metadata]

      expect {
        one(shard2).getMetadata(sourceId) willReturn Some(metadata)
        one(shard2).add(sourceId, Time.now)
      }

      val fake2 = new FakeLockingShard(shard2)
      val fake1 = new FakeReadWriteShard[Shard](fake2, null, 1, Nil)
      val shard = new ReadWriteShardAdapter(fake1)
      shard.withLock(sourceId) { (innerShard, metadata) =>
        innerShard.add(sourceId, Time.now)
      }
    }

    "coalesce metadata" in {
      val child1 = mock[Shard]
      val child2 = mock[Shard]
      val child3 = mock[Shard]
      val children = List(child1, child2, child3)
      val fake1 = new ReplicatingShard[Shard](null, 1, children, () => children, null)
      val shard = new ReadWriteShardAdapter(fake1)

      "with a clear time winner" in {
        val metadata1 = Metadata(3, State.Removed, 1, Time(3.seconds))
        val metadata2 = Metadata(3, State.Archived, 1, Time(3.seconds))
        val metadata3 = Metadata(3, State.Normal, 1, Time(4.seconds))

        expect {
          one(child1).getMetadata(3) willReturn Some(metadata1)
          one(child2).getMetadata(3) willReturn Some(metadata2)
          one(child3).getMetadata(3) willReturn Some(metadata3)
        }

        shard.getMetadata(3) mustEqual Some(metadata3)
      }

      "with the same time" in {
        val metadata1 = Metadata(3, State.Removed, 1, Time(3.seconds))
        val metadata2 = Metadata(3, State.Archived, 1, Time(3.seconds))
        val metadata3 = Metadata(3, State.Normal, 1, Time(3.seconds))

        expect {
          one(child1).getMetadata(3) willReturn Some(metadata1)
          one(child2).getMetadata(3) willReturn Some(metadata2)
          one(child3).getMetadata(3) willReturn Some(metadata3)
        }

        shard.getMetadata(3) mustEqual Some(metadata1)
      }
    }
  }
}
