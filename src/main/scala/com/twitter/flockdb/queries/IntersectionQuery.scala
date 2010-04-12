package com.twitter.flockdb.queries

import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.results.{Cursor, ResultWindow}
import net.lag.configgy.Configgy



class IntersectionQuery(query1: Query, query2: Query) extends Query {
  val config = Configgy.config
  val count1 = query1.sizeEstimate
  val count2 = query2.sizeEstimate

  val (smallerQuery, largerQuery) = if (count1 < count2) {
    (query1, query2)
  } else {
    (query2, query1)
  }

  def sizeEstimate() = ((count1 min count2) * config("edges.average_intersection_proportion").toDouble).toInt

  def selectPage(count: Int, cursor: Cursor) = selectPageByDestinationId(count, cursor)

  def selectPageByDestinationId(count: Int, cursor: Cursor) = {
    if (count1 == 0 || count2 == 0) {
      new ResultWindow(List[(Long,Cursor)](), count, cursor)
    } else {
      val guessedPageSize = (count / config("edges.average_intersection_proportion").toDouble).toInt
      val internalPageSize = guessedPageSize min config("edges.intersection_page_size_max").toInt

      var resultWindow = pageIntersection(smallerQuery, largerQuery, internalPageSize, count, cursor)
      while (resultWindow.page.size < count && resultWindow.continueCursor != Cursor.End) {
        resultWindow = resultWindow ++ pageIntersection(smallerQuery, largerQuery, internalPageSize, count, resultWindow.continueCursor)
      }
      resultWindow
    }
  }

  def selectWhereIn(page: Seq[Long]) = largerQuery.selectWhereIn(smallerQuery.selectWhereIn(page))

  private def pageIntersection(smallerQuery: Query, largerQuery: Query, internalPageSize: Int, count: Int, cursor: Cursor) = {
    val results = smallerQuery.selectPageByDestinationId(internalPageSize, cursor)
    val whereIn = largerQuery.selectWhereIn(results.projection)
    new ResultWindow(Cursor.cursorZip(whereIn), results.nextCursor, results.prevCursor, count, cursor)
  }
}
