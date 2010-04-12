package com.twitter.flockdb.shards

import com.twitter.gizzard.shards
import com.twitter.ostrich.StatsProvider


class StatsCollectingShardFactory(factory: shards.ShardFactory[Shard], stats: StatsProvider)
  extends shards.ShardFactory[Shard] {
  def instantiate(shardInfo: shards.ShardInfo, weight: Int, children: Seq[Shard]) =
    new StatsCollectingShard(shardInfo, weight, List(factory.instantiate(shardInfo, weight, children)), stats)
  def materialize(shardInfo: shards.ShardInfo) = factory.materialize(shardInfo)
}

class StatsCollectingShard(shardInfo: shards.ShardInfo, weight: Int, children: Seq[Shard], stats: StatsProvider)
  extends shards.StatsCollectingShard[Shard](shardInfo, weight, children, stats) with ReadWriteShard
