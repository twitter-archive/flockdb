package com.twitter.flockdb.queries

import com.twitter.results.Cursor
import net.lag.configgy.Configgy


class DifferenceQuery(query1: Query, query2: Query) extends Query {
  val config = Configgy.config

  def sizeEstimate = query1.sizeEstimate

  def selectPage(count: Int, cursor: Cursor) = selectPageByDestinationId(count, cursor)

  def selectPageByDestinationId(count: Int, cursor: Cursor) = {
    val guessedPageSize = (count + count * config("edges.average_intersection_proportion").toDouble).toInt
    val internalPageSize = guessedPageSize min config("edges.intersection_page_size_max").toInt

    var resultWindow = pageDifference(internalPageSize, count, cursor)
    while (resultWindow.page.size < count && resultWindow.continueCursor != Cursor.End) {
      resultWindow = resultWindow ++ pageDifference(internalPageSize, count, resultWindow.continueCursor)
    }
    resultWindow
  }

  def selectWhereIn(page: Seq[Long]) = {
    val results = query1.selectWhereIn(page)
    val rejects = Set(query2.selectWhereIn(results): _*)
    results.filter { item => !rejects.contains(item) }
  }

  private def pageDifference(internalPageSize: Int, count: Int, cursor: Cursor) = {
    val results = query1.selectPageByDestinationId(internalPageSize, cursor)
    val rejects = query2.selectWhereIn(results.projection)
    results -- rejects
  }
}
