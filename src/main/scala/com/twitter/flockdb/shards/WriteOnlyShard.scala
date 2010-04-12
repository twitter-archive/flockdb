package com.twitter.flockdb.shards

import com.twitter.gizzard.shards


class WriteOnlyShardFactory extends shards.ShardFactory[Shard] {
  def instantiate(shardInfo: shards.ShardInfo, weight: Int, children: Seq[Shard]) =
    new WriteOnlyShard(shardInfo, weight, children)
  def materialize(shardInfo: shards.ShardInfo) = ()
}

class WriteOnlyShard(shardInfo: shards.ShardInfo, weight: Int, children: Seq[Shard])
  extends shards.WriteOnlyShard[Shard](shardInfo, weight, children) with ReadWriteShard
