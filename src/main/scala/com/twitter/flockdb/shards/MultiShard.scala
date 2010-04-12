package com.twitter.flockdb.shards

import collection.mutable


class MultiShard(forwardingManager: ForwardingManager, sourceIds: Seq[Long], graphId: Int, direction: Direction) {
  private val shards = new mutable.HashMap[Shard, mutable.ArrayBuffer[Long]]
  for (id <- sourceIds) {
    val shard = forwardingManager.find(id, graphId, direction)
    shards.getOrElseUpdate(shard, new mutable.ArrayBuffer[Long]) += id
  }

  def counts = {
    val results = new mutable.HashMap[Long, Int]
    for ((shard, ids) <- shards) shard.counts(ids, results)
    sourceIds.map[Int] { results.getOrElse(_, 0) }
  }
}
