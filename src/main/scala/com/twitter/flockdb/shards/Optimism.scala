package com.twitter.flockdb.shards

import com.twitter.gizzard.shards.ShardException
import com.twitter.util.Time

class OptimisticLockException(message: String) extends ShardException(message)

trait Optimism extends Shard {
  def optimistically(sourceId: Long)(f: State => Unit) = {
    val before = getWinner(sourceId)
    f(before.state)
    val after = getWinner(sourceId)
    if(before.state != after.state) {
      throw new OptimisticLockException("Lost optimistic lock for " + sourceId + ": was " + before.state +", now " + after.state)
    }
  }

  def getWinner(sourceId: Long) = {
    // The default metadata
    var winning = Metadata(sourceId, State.Normal, 0, new Time(0))

    val metadatas = getMetadatas(sourceId)
    metadatas.foreach { optionMetadata =>
      optionMetadata.foreach { metadata =>
        winning = metadata max winning
      }
    }
    winning
  }
}