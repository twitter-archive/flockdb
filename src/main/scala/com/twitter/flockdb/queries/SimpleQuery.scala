package com.twitter.flockdb.queries

import scala.util.Sorting
import com.twitter.results.Cursor
import com.twitter.service.flock.State
import net.lag.configgy.Configgy
import flockdb.shards.Shard


class SimpleQuery(shard: Shard, sourceId: Long, states: Seq[State]) extends Query {
  val config = Configgy.config

  def sizeEstimate() = shard.count(sourceId, states)

  def selectWhereIn(page: Seq[Long]) = shard.intersect(sourceId, states, page)

  def selectPageByDestinationId(count: Int, cursor: Cursor) = {
    shard.selectByDestinationId(sourceId, states, count, cursor)
  }

  def selectPage(count: Int, cursor: Cursor) = {
    shard.selectByPosition(sourceId, states, count, cursor)
  }
}
