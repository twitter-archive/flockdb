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
import com.twitter.flockdb.jobs.single._
import com.twitter.util.Time
import com.twitter.util.TimeConversions._
import org.specs.mock.{ClassMocker, JMocker}
import jobs.{Repair, MetadataRepair}
import shards.{Metadata, Shard}
import com.twitter.flockdb.jobs.multi._

class RepairSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  val shard1Id = ShardId("test", "shard1")
  val shard2Id = ShardId("test", "shard2")
  val count = 2300

  "Repair" should {
    val cursor1 = Cursor(1L)
    val cursor2 = Cursor(2L)
    var tableId = 0
    val nameServer = mock[NameServer[Shard]]
    val scheduler = mock[PrioritizingJobScheduler[JsonJob]]
    val shard1 = mock[Shard]
    val shard2 = mock[Shard]
    val job = new Repair(shard1Id, shard2Id, tableId, (cursor1, cursor2), (cursor1, cursor2), count, nameServer, scheduler)

    "resolve trivial edges" in {
      expect {
        one(nameServer).findShardById(shard1Id) willReturn shard1
        one(nameServer).findShardById(shard2Id) willReturn shard2
        one(shard1).selectAll((cursor1, cursor2), count) willReturn (
          List(new Edge(1L, 2L, 3L, Time.now, 5, State.Normal)), (cursor1, Cursor(cursor2.position + 1))
        )
        one(shard2).selectAll((cursor1, cursor2), count) willReturn (
          List(new Edge(1L, 2L, 3L, Time.now, 5, State.Normal)), (cursor1, Cursor(cursor2.position + 1))
        )
        one(scheduler).put(Priority.Medium.id, new Repair(shard1Id, shard2Id, tableId, (cursor1, Cursor(cursor2.position + 1)), (cursor1, Cursor(cursor2.position + 1)), count, nameServer, scheduler))
      }
      job.apply()
    }

    "resolve normal, archived edges" in {
      val (add, archive) = (capturingParam[Add], capturingParam[com.twitter.flockdb.jobs.single.Archive])
      expect {
        one(nameServer).findShardById(shard1Id) willReturn shard1
        one(nameServer).findShardById(shard2Id) willReturn shard2
        one(shard1).selectAll((cursor1, cursor2), count) willReturn (
          List(new Edge(1L, 2L, 3L, Time.now, 5, State.Normal)), (cursor1, Cursor(cursor2.position + 1))
        )
        one(shard2).selectAll((cursor1, cursor2), count) willReturn (
          List(new Edge(1L, 2L, 3L, Time.now, 5, State.Archived)), (cursor1, Cursor(cursor2.position + 1))
        )
        one(scheduler).put(will(beEqual(Priority.Medium.id)), add.capture)
        one(scheduler).put(will(beEqual(Priority.Medium.id)), archive.capture)
        
        one(scheduler).put(Priority.Medium.id, new Repair(shard1Id, shard2Id, tableId, (cursor1, Cursor(cursor2.position + 1)), (cursor1, Cursor(cursor2.position + 1)), count, nameServer, scheduler))
      }
      job.apply()
      add.captured.sourceId must_== 1L
      add.captured.destinationId must_== 2L
      archive.captured.sourceId must_== 1L
      archive.captured.destinationId must_== 2L
    }

    "resolve missing edge" in {
      val add = capturingParam[Add]
      expect {
        one(nameServer).findShardById(shard1Id) willReturn shard1
        one(nameServer).findShardById(shard2Id) willReturn shard2
        one(shard1).selectAll((cursor1, cursor2), count) willReturn (
          List[Edge](), (Cursor.End, Cursor.End)
        )
        one(shard2).selectAll((cursor1, cursor2), count) willReturn (
          List(new Edge(1L, 2L, 3L, Time.now, 5, State.Normal)), (Cursor.End, Cursor.End)
        )
        one(scheduler).put(will(beEqual(Priority.Medium.id)), add.capture)
      }
      job.apply()
      add.captured.sourceId must_== 1L
      add.captured.destinationId must_== 2L
    }

    "reschedule when you run out, but you're not at the end of the cursor" in {
      expect {
        one(nameServer).findShardById(shard1Id) willReturn shard1
        one(nameServer).findShardById(shard2Id) willReturn shard2
        one(shard1).selectAll((cursor1, cursor2), count) willReturn (
          List[Edge](), (Cursor(5L), Cursor(5L))
        )
        one(shard2).selectAll((cursor1, cursor2), count) willReturn (
          List(new Edge(1L, 2L, 3L, Time.now, 5, State.Normal)), (Cursor(1L), Cursor(3L))
        )
        one(scheduler).put(Priority.Medium.id, new Repair(shard1Id, shard2Id, tableId, (Cursor(1L), Cursor(2L)), (Cursor(1L), Cursor(2L)), count, nameServer, scheduler))
      }
      job.apply()
    }
  }

  "MetadataRepair" should {
    val cursor1 = Cursor(1L)
    var tableId = 0
    val nameServer = mock[NameServer[Shard]]
    val scheduler = mock[PrioritizingJobScheduler[JsonJob]]
    val shard1 = mock[Shard]
    val shard2 = mock[Shard]
    val job = new MetadataRepair(shard1Id, shard2Id, tableId, cursor1, cursor1, count, nameServer, scheduler)

    "resolve trivial metadata" in {
      expect {
        one(nameServer).findShardById(shard1Id) willReturn shard1
        one(nameServer).findShardById(shard2Id) willReturn shard2
        one(shard1).selectAllMetadata(cursor1, count) willReturn (
          List(new Metadata(1L, State.Normal, 0, Time.now)), Cursor(cursor1.position + 1)
        )
        one(shard2).selectAllMetadata(cursor1, count) willReturn (
          List(new Metadata(1L, State.Normal, 0, Time.now)), Cursor(cursor1.position + 1)
        )
        one(scheduler).put(Priority.Medium.id, new MetadataRepair(shard1Id, shard2Id, tableId, Cursor(cursor1.position + 1), Cursor(cursor1.position + 1), count, nameServer, scheduler))
      }
      job.apply()
    }

    "resolve normal, archived metadata" in {
      val (add, archive) = (capturingParam[Unarchive], capturingParam[com.twitter.flockdb.jobs.multi.Archive])
      expect {
        one(nameServer).findShardById(shard1Id) willReturn shard1
        one(nameServer).findShardById(shard2Id) willReturn shard2
        
        one(shard1).selectAllMetadata(cursor1, count) willReturn (
          List(new Metadata(1L, State.Normal, 0, Time.now)), Cursor(cursor1.position + 1)
        )
        one(shard2).selectAllMetadata(cursor1, count) willReturn (
          List(new Metadata(1L, State.Archived, 0, Time.now)), Cursor(cursor1.position + 1)
        )
        one(scheduler).put(will(beEqual(Priority.Medium.id)), add.capture)
        one(scheduler).put(will(beEqual(Priority.Medium.id)), archive.capture)
        
        one(scheduler).put(Priority.Medium.id, new MetadataRepair(shard1Id, shard2Id, tableId, Cursor(cursor1.position + 1), Cursor(cursor1.position + 1), count, nameServer, scheduler))
      }
      job.apply()
      add.captured.sourceId must_== 1L
      archive.captured.sourceId must_== 1L
    }

    "resolve missing metadata" in {
      val add1 = capturingParam[Unarchive]
      expect {
        one(nameServer).findShardById(shard1Id) willReturn shard1
        one(nameServer).findShardById(shard2Id) willReturn shard2
        
        one(shard1).selectAllMetadata(cursor1, count) willReturn (
          List(new Metadata(1L, State.Normal, 0, Time.now)), MetadataRepair.END
        )
        one(shard2).selectAllMetadata(cursor1, count) willReturn (
          List[Metadata](), MetadataRepair.END
        )
        one(scheduler).put(will(beEqual(Priority.Medium.id)), add1.capture)
        
        one(scheduler).put(Priority.Medium.id, new Repair(shard1Id, shard2Id, tableId, Repair.START, Repair.START, Repair.COUNT, nameServer, scheduler))
      }
      job.apply()
      add1.captured.sourceId must_== 1L
    }
  }
}
