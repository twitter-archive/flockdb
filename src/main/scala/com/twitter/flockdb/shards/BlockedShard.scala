package com.twitter.flockdb.shards

import com.twitter.gizzard.shards


class BlockedShardFactory extends shards.ShardFactory[Shard] {
  def instantiate(shardInfo: shards.ShardInfo, weight: Int, children: Seq[Shard]) =
    new BlockedShard(shardInfo, weight, children)
  def materialize(shardInfo: shards.ShardInfo) = ()
}

class BlockedShard(shardInfo: shards.ShardInfo, weight: Int, children: Seq[Shard])
  extends shards.BlockedShard[Shard](shardInfo, weight, children) with ReadWriteShard
