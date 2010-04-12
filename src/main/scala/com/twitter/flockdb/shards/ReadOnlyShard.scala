package com.twitter.flockdb.shards

import com.twitter.gizzard.shards


class ReadOnlyShardFactory extends shards.ShardFactory[Shard] {
  def instantiate(shardInfo: shards.ShardInfo, weight: Int, children: Seq[Shard]) =
    new ReadOnlyShard(shardInfo, weight, children)
  def materialize(shardInfo: shards.ShardInfo) = ()
}

class ReadOnlyShard(shardInfo: shards.ShardInfo, weight: Int, children: Seq[Shard])
  extends shards.ReadOnlyShard[Shard](shardInfo, weight, children) with ReadWriteShard
