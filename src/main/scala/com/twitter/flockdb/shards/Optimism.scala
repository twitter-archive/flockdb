package com.twitter.flockdb.shards

import com.twitter.gizzard.shards.ShardException
import com.twitter.util.Time

class OptimisticLockException(message: String) extends ShardException(message)

trait Optimism extends Shard {
  protected case class MetadataWithEx(metadata: Metadata, ex: Option[Throwable])

  def optimistically(sourceId: Long)(f: State => Unit) = {

    var before = getWinner(sourceId)

    f(before.metadata.state)

    // We didn't do this immediately, because we still want to propagate writes with best effort.
    // We should reenqueue if the optimistic lock only covers a subset of the intended targets.
    before.ex.foreach(throw _)

    var after = getWinner(sourceId)
    after.ex.foreach(throw _)

    if(before.metadata.state != after.metadata.state) {
      throw new OptimisticLockException("Lost optimistic lock for " + sourceId + ": was " + before.metadata.state +", now " + after.metadata.state)
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
    MetadataWithEx(winning, metadatas.exceptions.headOption)
  }
}