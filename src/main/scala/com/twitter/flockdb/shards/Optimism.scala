package com.twitter.flockdb
package shards

import com.twitter.gizzard.shards.ShardException
import com.twitter.util.Time
import com.twitter.logging.Logger

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
 *
 * The current iteration of the optimism branch is (perhaps overly) conservative by maintaining
 * a lock over all replicas. We could choose to go a couple different ways from here:
 *
 * 1. Hold the course.  This works.
 * 2. Only lock over (any) one server of each replication set (so two total--one forwards, one
 *    backwards).  This would reduce the number of reads.  If our data is (a) eventually-but-not-
 *    yet-consistent and not just corrupted, and (b) two copies of a node have different states,
 *    then the job to update the state of the node to the newer state is still active.  If we write
 *    the wrong edge color based upon the old node, it is true that there is a multi job in some
 *    presently existing queue that will fix the edge.
 * 3. Do read repair.  When we do the optimistic lock read over all replicas, we're already grabbing
 *    all of the information we need to repair the graphs.  Instead of just throwing an exception,
 *    or ignoring the inconsistency, we could enqueue the appropriate multi job to perform the repair.
 *    My chief concern would be that we could issue a storm of multi jobs.
 *
 * Eventually, we will probably choose (2) or (3), depending on how we weigh the tradeoff between
 * repairing consistency and raw performance.  However, we should stick to (1) for now, and evaluate
 * the other options soon.
 *
 * Also, in the short term, it's worth understanding (2), so that you can realize that adding
 * replicas doesn't screw things up.
 */
trait Optimism extends Shard {
  private val log = Logger.get(getClass.getName)

  def optimistically(sourceId: Long)(f: State => Unit) = {
    try {
      log.debug("starting optimistic lock of " + shardInfo.id + " for " + sourceId)

      val (beforeStateOpt, beforeEx) = getDominantState(sourceId)
      val beforeState = beforeStateOpt.getOrElse(State.Normal)

      if (beforeStateOpt.isEmpty) beforeEx.foreach(throw _)

      f(beforeState)

      // We didn't do this immediately if we got a result from one shard, because we still want to propagate writes with best effort.
      // We should reenqueue if the optimistic lock only covers a subset of the intended targets.
      beforeEx.foreach(throw _)

      val (afterStateOpt, afterEx) = getDominantState(sourceId)
      val afterState = afterStateOpt.getOrElse(State.Normal)

      afterEx.foreach(throw _)

      if(beforeState != afterState) {
        val msg = shardInfo.id + " lost optimistic lock for " + sourceId + ": was " + beforeState +", now " + afterState
        log.debug(msg)

        throw new OptimisticLockException(msg)
      }

      log.debug("successful optimistic lock of " + shardInfo.id + " for " + sourceId)

    } catch {
      case e => {
        log.debug("exception in optimistic lock of " + shardInfo.id + " for " + sourceId + ": " + e.getMessage)
        throw e
      }
    }
  }

  def getDominantState(sourceId: Long) = {
    // The default metadata
    var winning: Option[Metadata]   = None
    var exceptions: List[Throwable] = Nil

    getMetadatas(sourceId).foreach {
      case Left(ex)              => exceptions = ex :: exceptions
      case Right(Some(metadata)) => winning    = winning.map(_ max metadata).orElse(Some(metadata))
      case Right(None)           => ()
    }

    (winning.map(_.state), exceptions.headOption)
  }
}
