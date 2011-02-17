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

import com.twitter.gizzard.nameserver.NameServer
import com.twitter.gizzard.scheduler._
import com.twitter.gizzard.shards.{Busy, ShardId, ShardInfo, ShardTimeoutException}
import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.util.Time
import com.twitter.util.TimeConversions._
import org.specs.mock.{ClassMocker, JMocker}
import jobs.{Copy, MetadataCopy}
import shards.{Shard}

class CopySpec extends ConfiguredSpecification with JMocker with ClassMocker {
  val shard1Id = ShardId("test", "shard1")
  val shard2Id = ShardId("test", "shard2")
  val shard1Info = new ShardInfo("TestShard", "shard1", "test")
  val shard2Info = new ShardInfo("TestShard", "shard2", "test")
  val count = 2300

  "Copy" should {
    val cursor1 = Cursor(337L)
    val cursor2 = Cursor(555L)
    val nameServer = mock[NameServer[Shard]]
    val scheduler = mock[JobScheduler[JsonJob]]
    val shard1 = mock[Shard]
    val shard2 = mock[Shard]

    "apply" in {
      val job = new Copy(shard1Id, shard2Id, (cursor1, cursor2), count, nameServer, scheduler)
      val edge = new Edge(1L, 2L, 3L, Time.now, 5, State.Normal)

      "continuing work" >> {
        expect {
          one(nameServer).getShard(shard2Id) willReturn shard2Info
          one(nameServer).markShardBusy(shard2Id, Busy.Busy)
          one(nameServer).findShardById(shard1Id) willReturn shard1
          one(nameServer).findShardById(shard2Id) willReturn shard2
          one(shard1).selectAll((cursor1, cursor2), count) willReturn (List(edge), (cursor1, Cursor(cursor2.position + 1)))
          one(shard2).writeCopies(List(edge))
          one(scheduler).put(new Copy(shard1Id, shard2Id, (cursor1, Cursor(cursor2.position + 1)), count, nameServer, scheduler))
        }
        job.apply()
      }

      "try again on timeout" >> {
        expect {
          one(nameServer).getShard(shard2Id) willReturn shard2Info
          one(nameServer).markShardBusy(shard2Id, Busy.Busy)
          one(nameServer).findShardById(shard1Id) willReturn shard1
          one(nameServer).findShardById(shard2Id) willReturn shard2
          one(shard1).selectAll((cursor1, cursor2), count) willThrow new ShardTimeoutException(100.milliseconds, null)
          one(scheduler).put(new Copy(shard1Id, shard2Id, (cursor1, cursor2), (count.toFloat * 0.9).toInt, nameServer, scheduler))
        }
        job.apply()
     }

      "finished" >> {
        expect {
          one(nameServer).getShard(shard2Id) willReturn shard2Info
          one(nameServer).markShardBusy(shard2Id, Busy.Busy)
          one(nameServer).findShardById(shard1Id) willReturn shard1
          one(nameServer).findShardById(shard2Id) willReturn shard2
          one(shard1).selectAll((cursor1, cursor2), count) willReturn (List(edge), (Cursor.End, Cursor.End))
          one(shard2).writeCopies(List(edge))
          one(nameServer).markShardBusy(shard2Id, Busy.Normal)
        }
        job.apply()
      }

    }

    "toJson" in {
      val job = new Copy(shard1Id, shard2Id, (cursor1, cursor2), count, nameServer, scheduler)
      val json = job.toJson
      json mustMatch "Copy"
      json mustMatch "\"cursor1\":" + cursor1.position
      json mustMatch "\"cursor2\":" + cursor2.position
    }
  }

  "MetadataCopy" should {
    val cursor = Cursor(1L)
    val nameServer = mock[NameServer[Shard]]
    val scheduler = mock[JobScheduler[JsonJob]]
    val shard1 = mock[Shard]
    val shard2 = mock[Shard]
    val job = new MetadataCopy(shard1Id, shard2Id, cursor, count, nameServer, scheduler)

    "apply" in {
      "continuing work" >> {
        val metadata = new Metadata(1, State.Normal, 2, Time.now)
        expect {
          one(nameServer).getShard(shard2Id) willReturn shard2Info
          one(nameServer).markShardBusy(shard2Id, Busy.Busy)
          one(nameServer).findShardById(shard1Id) willReturn shard1
          one(nameServer).findShardById(shard2Id) willReturn shard2
          one(shard1).selectAllMetadata(cursor, count) willReturn (List(metadata), Cursor(cursor.position + 1))
          one(shard2).writeMetadata(List(metadata))
          one(scheduler).put(new MetadataCopy(shard1Id, shard2Id, Cursor(cursor.position + 1), count, nameServer, scheduler))
        }
        job.apply()
      }

      "finished" >> {
        val metadata = new Metadata(1, State.Normal, 2, Time.now)
        expect {
          one(nameServer).getShard(shard2Id) willReturn shard2Info
          one(nameServer).findShardById(shard1Id) willReturn shard1
          one(nameServer).findShardById(shard2Id) willReturn shard2
          one(shard1).selectAllMetadata(cursor, count) willReturn (List(metadata), Cursor.End)
          one(shard2).writeMetadata(List(metadata))
          one(nameServer).markShardBusy(shard2Id, Busy.Busy)
          one(scheduler).put(new Copy(shard1Id, shard2Id, (Cursor.Start, Cursor.Start), Copy.COUNT, nameServer, scheduler))
        }
        job.apply()
      }
    }

    "toJson" in {
      val json = job.toJson
      json mustMatch "MetadataCopy"
      json mustMatch "\"cursor\":" + cursor.position
    }
  }
}
