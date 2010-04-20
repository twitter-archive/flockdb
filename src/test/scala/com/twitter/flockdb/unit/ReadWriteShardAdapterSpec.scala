package com.twitter.flockdb.unit

import com.twitter.gizzard.shards.{ReadWriteShard, ShardInfo}
import com.twitter.xrayspecs.Time
import org.specs.mock.{ClassMocker, JMocker}
import shards.{ReadWriteShardAdapter, Shard, Metadata}
import thrift.Edge


class FakeReadWriteShard(shard: Shard, val shardInfo: ShardInfo, val weight: Int, val children: Seq[Shard]) extends ReadWriteShard[Shard] {
  def readOperation[A](method: (Shard => A)): A = method(shard)
  def writeOperation[A](method: (Shard => A)): A = method(shard)
}

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
        one(shard1).add(sourceId, Time.now)
      }

      val fake2 = new FakeLockingShard(shard2)
      val fake3 = new FakeLockingShard(shard3)
      val fake1 = new FakeReadWriteShard(shard1, null, 1, List[Shard](fake2, fake3))
      val shard = new ReadWriteShardAdapter(fake1)
      shard.withLock(sourceId) { (innerShard, metadata) =>
        innerShard.add(sourceId, Time.now)
      }
    }
  }
}
