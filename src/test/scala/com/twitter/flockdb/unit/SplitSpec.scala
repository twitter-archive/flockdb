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
import com.twitter.gizzard.scheduler._
import com.twitter.gizzard.shards.{Busy, ShardId, ShardTimeoutException}
import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.util.Time
import com.twitter.util.TimeConversions._
import org.specs.mock.{ClassMocker, JMocker}
import jobs._
import shards.{Metadata, Shard}

class SplitSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  val shard1Id = ShardId("test", "shard1")
  val shard2aId = ShardId("test", "shard2")
  val shard2bId = ShardId("test", "shard2")
  val dests = List(CopyDestination(shard2aId, Some(0)), CopyDestination(shard2bId, Some(50)))
  val count = 2300
  val shard1 = mock[Shard]
  val shard2a = mock[Shard]
  val shard2b = mock[Shard]
  val nameServer = mock[NameServer[Shard]]
  val scheduler = mock[JobScheduler[JsonJob]]

  "Split" should {
    val cursor1 = Cursor(0)
    val cursor2 = Cursor(0)

    "apply" in {
      val job = new Split(shard1Id, dests, (cursor1, cursor2), count, nameServer, scheduler)
      val edgeA = new Edge(1L, 2L, 3L, Time.now, 5, State.Normal)
      val edgeB = new Edge(2L, 2L, 3L, Time.now, 5, State.Normal)
      val edgeC = new Edge(100L, 2L, 3L, Time.now, 5, State.Normal)

      "write the correct partial to each shard" >> {
        expect {
          allowing(nameServer).mappingFunction willReturn ((a: Long) => a)
          one(nameServer).markShardBusy(shard2aId, Busy.Busy)
          one(nameServer).markShardBusy(shard2bId, Busy.Busy)
          one(nameServer).findShardById(shard1Id) willReturn shard1
          one(nameServer).findShardById(shard2aId) willReturn shard2a
          one(nameServer).findShardById(shard2bId) willReturn shard2b
          one(shard1).selectAll((cursor1, cursor2), count) willReturn (List(edgeA, edgeB, edgeC), (cursor1, Cursor(cursor2.position + 1)))
          one(shard2a).writeCopies(List(edgeA, edgeB))
          one(shard2b).writeCopies(List(edgeC))
          one(scheduler).put(new Split(shard1Id, dests, (cursor1, Cursor(cursor2.position + 1)), count, nameServer, scheduler))
        }
        job.apply()
      }
    }

  }

  "MetadataSplit" should {
    val cursor = Cursor(0)
    val job = new MetadataSplit(shard1Id, dests, cursor, count, nameServer, scheduler)
    val metaA = Metadata(1L, State.Normal, 1, Time.now)
    val metaB = Metadata(2L, State.Normal, 1, Time.now)
    val metaC = Metadata(100L, State.Normal, 1, Time.now)

    "apply" in {
      "write the correct partial to each shard" >> {
        expect {
          allowing(nameServer).mappingFunction willReturn ((a: Long) => a)
          one(nameServer).markShardBusy(shard2aId, Busy.Busy)
          one(nameServer).markShardBusy(shard2bId, Busy.Busy)
          one(nameServer).findShardById(shard1Id) willReturn shard1
          one(nameServer).findShardById(shard2aId) willReturn shard2a
          one(nameServer).findShardById(shard2bId) willReturn shard2b
          one(shard1).selectAllMetadata(cursor, count) willReturn (List(metaA, metaB, metaC), Cursor(cursor.position + 1))
          one(shard2a).writeMetadata(metaA)
          one(shard2a).writeMetadata(metaB)
          one(shard2b).writeMetadata(metaC)
          one(scheduler).put(new MetadataCopy(shard1Id, dests, Cursor(cursor.position + 1), count, nameServer, scheduler))
        }
        job.apply()
      }
    }
  }
}
