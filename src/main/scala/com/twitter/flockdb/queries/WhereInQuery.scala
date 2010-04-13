package com.twitter.flockdb.queries

import scala.util.Sorting
import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.results.{Cursor, ResultWindow}
import flockdb.shards.Shard


class WhereInQuery(shard: Shard, sourceId: Long, states: Seq[State], destinationIds: Seq[Long]) extends Query {
  def sizeEstimate() = destinationIds.size

  def selectWhereIn(page: Seq[Long]) = {
    shard.intersect(sourceId, states, (Set(destinationIds: _*) intersect Set(page: _*)).toSeq)
  }

  def selectPageByDestinationId(count: Int, cursor: Cursor) = {
    val results = shard.intersect(sourceId, states, destinationIds)
    new ResultWindow(results.map(result => (result, Cursor(result))), count, cursor)
  }

  def selectPage(count: Int, cursor: Cursor) = selectPageByDestinationId(count, cursor)
}
