package com.twitter.flockdb.shards

import com.twitter.gizzard.shards.ShardException
import com.twitter.util.Time

class OptimisticLockException(message: String) extends ShardException(message)

/**
 * This handles a new optimistic lock of one direction of an edge.  In order to lock an
 * entire edge, you should nest two of these--one for each direction.  This lock is *only*
 * responsible for maintaining color/state consistency.
 *
 * Inconsistencies between replicas are handled by consensus.  The most recent and therefore
 * highest priority row wins.  It helps to visualize each replication set as a unified state
 * of the union of all rows, taking the highest priority rows where conflicts happen.  If two
 * rows are different, and we play an operation, that operation can't make things worse, because
 * it will move the timeline forwards.
 *
 * This lock does a read across every available replica. If any read fails, the entire
 * lock fails (i.e. reenqueues happen).  However, the writes will still proceed first.  We
 * want to propagate writes, as they're "better" than the old state, while still not
 * neccessarily being the final state.
 *
 * There would be two ways to make things worse:
 * 1. regress the timeline (by writing an old operation, which is impossible under the contract
 *    of the shards themselves)
 * 2. write a color that doesn't represent the current state of the consensus at some time at or
 *    after the row is written.  The key case to worry about here is when the consensus looks like
 *    Seq(old_row, Offline(new_row)).  In this case, the non-unanimous consensus would write the
 *    wrong color.  We require that the consensus be unanimous for this reason.
 */
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