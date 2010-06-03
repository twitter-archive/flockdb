package com.twitter.flockdb.unit

import com.twitter.gizzard.scheduler.JobScheduler
import com.twitter.gizzard.nameserver.{NameServer, ShardMigration}
import com.twitter.gizzard.shards.{Busy, ShardId, ShardTimeoutException}
import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.results.Cursor
import com.twitter.xrayspecs.Time
import com.twitter.xrayspecs.TimeConversions._
import org.specs.mock.{ClassMocker, JMocker}
import jobs.{Copy, MetadataCopy}
import shards.{Metadata, Shard}


object CopySpec extends ConfiguredSpecification with JMocker with ClassMocker {
  val shard1Id = ShardId("test", "shard1")
  val shard2Id = ShardId("test", "shard2")
  val count = 2300

  "Copy" should {
    val cursor1 = Cursor(337L)
    val cursor2 = Cursor(555L)
    val nameServer = mock[NameServer[Shard]]
    val scheduler = mock[JobScheduler]
    val shard1 = mock[Shard]
    val shard2 = mock[Shard]

    "apply" in {
      val job = new Copy(shard1Id, shard2Id, (cursor1, cursor2), count)
      val edge = new Edge(1L, 2L, 3L, Time.now, 5, State.Normal)

      "continuing work" >> {
        expect {
          one(nameServer).markShardBusy(shard2Id, Busy.Busy)
          one(nameServer).findShardById(shard1Id) willReturn shard1
          one(nameServer).findShardById(shard2Id) willReturn shard2
          one(shard1).selectAll((cursor1, cursor2), count) willReturn (List(edge), (cursor1, Cursor(cursor2.position + 1)))
          one(shard2).writeCopies(List(edge))
          one(scheduler).apply(new Copy(shard1Id, shard2Id, (cursor1, Cursor(cursor2.position + 1)), count))
        }
        job.apply((nameServer, scheduler))
      }

      "try again on timeout" >> {
        expect {
          one(nameServer).markShardBusy(shard2Id, Busy.Busy)
          one(nameServer).findShardById(shard1Id) willReturn shard1
          one(nameServer).findShardById(shard2Id) willReturn shard2
          one(shard1).selectAll((cursor1, cursor2), count) willThrow new ShardTimeoutException
          one(scheduler).apply(new Copy(shard1Id, shard2Id, (cursor1, cursor2), count / 2))
        }
        job.apply((nameServer, scheduler))
     }

      "finished" >> {
        expect {
          one(nameServer).markShardBusy(shard2Id, Busy.Busy)
          one(nameServer).findShardById(shard1Id) willReturn shard1
          one(nameServer).findShardById(shard2Id) willReturn shard2
          one(shard1).selectAll((cursor1, cursor2), count) willReturn (List(edge), (Cursor.End, Cursor.End))
          one(shard2).writeCopies(List(edge))
          one(nameServer).markShardBusy(shard2Id, Busy.Normal)
        }
        job.apply((nameServer, scheduler))
      }

    }

    "toJson" in {
      val job = new Copy(shard1Id, shard2Id, (cursor1, cursor2), count)
      val json = job.toJson
      json mustMatch "Copy"
      json mustMatch "\"source_shard_hostname\":\"%s\"".format(shard1Id.hostname)
      json mustMatch "\"source_shard_table_prefix\":\"%s\"".format(shard1Id.tablePrefix)
      json mustMatch "\"destination_shard_hostname\":\"%s\"".format(shard2Id.hostname)
      json mustMatch "\"destination_shard_table_prefix\":\"%s\"".format(shard2Id.tablePrefix)
      json mustMatch "\"count\":" + count
      json mustMatch "\"cursor1\":" + cursor1.position
      json mustMatch "\"cursor2\":" + cursor2.position
    }
  }

  "MetadataCopy" should {
    val cursor = Cursor(1L)
    val nameServer = mock[NameServer[Shard]]
    val scheduler = mock[JobScheduler]
    val shard1 = mock[Shard]
    val shard2 = mock[Shard]

    "apply" in {
      "continuing work" >> {
        val job = new MetadataCopy(shard1Id, shard2Id, cursor, count)
        val metadata = new Metadata(1, State.Normal, 2, Time.now)
        expect {
          one(nameServer).markShardBusy(shard2Id, Busy.Busy)
          one(nameServer).findShardById(shard1Id) willReturn shard1
          one(nameServer).findShardById(shard2Id) willReturn shard2
          one(shard1).selectAllMetadata(cursor, count) willReturn (List(metadata), Cursor(cursor.position + 1))
          one(shard2).writeMetadata(metadata)
          one(scheduler).apply(new MetadataCopy(shard1Id, shard2Id, Cursor(cursor.position + 1), count))
        }
        job.apply((nameServer, scheduler))
      }

      "finished" >> {
        val job = new MetadataCopy(shard1Id, shard2Id, cursor, count)
        val metadata = new Metadata(1, State.Normal, 2, Time.now)
        expect {
          one(nameServer).findShardById(shard1Id) willReturn shard1
          one(nameServer).findShardById(shard2Id) willReturn shard2
          one(shard1).selectAllMetadata(cursor, count) willReturn (List(metadata), Cursor.End)
          one(shard2).writeMetadata(metadata)
          one(nameServer).markShardBusy(shard2Id, Busy.Busy)
          one(scheduler).apply(new Copy(shard1Id, shard2Id, (Cursor.Start, Cursor.Start), Copy.COUNT))
        }
        job.apply((nameServer, scheduler))
      }
    }

    "toJson" in {
      val job = new MetadataCopy(shard1Id, shard2Id, cursor, count)
      val json = job.toJson
      json mustMatch "MetadataCopy"
      json mustMatch "\"source_shard_hostname\":\"%s\"".format(shard1Id.hostname)
      json mustMatch "\"source_shard_table_prefix\":\"%s\"".format(shard1Id.tablePrefix)
      json mustMatch "\"destination_shard_hostname\":\"%s\"".format(shard2Id.hostname)
      json mustMatch "\"destination_shard_table_prefix\":\"%s\"".format(shard2Id.tablePrefix)
      json mustMatch "\"count\":" + count
      json mustMatch "\"cursor\":" + cursor.position
    }
  }
}
