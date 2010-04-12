package com.twitter.flockdb.queries

import scala.util.Sorting
import net.lag.configgy.Configgy
import com.twitter.results.Cursor


class UnionQuery(query1: Query, query2: Query) extends Query {
  val config = Configgy.config

  def sizeEstimate() = query1.sizeEstimate max query2.sizeEstimate

  def selectPage(count: Int, cursor: Cursor) = selectPageByDestinationId(count, cursor)

  def selectPageByDestinationId(count: Int, cursor: Cursor) = {
    val result1 = query1.selectPageByDestinationId(count, cursor)
    val result2 = query2.selectPageByDestinationId(count, cursor)
    result1.merge(result2)
  }

  def selectWhereIn(page: Seq[Long]) = {
    merge(query1.selectWhereIn(page), query2.selectWhereIn(page))
  }

  private def merge(page1: Seq[Long], page2: Seq[Long]): Seq[Long] = {
    Sorting.stableSort((Set(page1: _*) ++ Set(page2: _*)).toSeq)
  }
}
