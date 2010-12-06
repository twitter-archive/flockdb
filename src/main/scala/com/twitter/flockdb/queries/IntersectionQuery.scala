/*
 * Copyright 2010 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.twitter.flockdb.queries

import com.twitter.util.Duration
import com.twitter.util.TimeConversions._
import com.twitter.gizzard.thrift.conversions.Sequences._

class IntersectionQuery(query1: Query, query2: Query, averageIntersectionProportion: Double, intersectionPageSizeMax: Int, intersectionTimeout: Duration) extends Query {
  val count1 = query1.sizeEstimate
  val count2 = query2.sizeEstimate

  val (smallerQuery, largerQuery) = if (count1 < count2) {
    (query1, query2)
  } else {
    (query2, query1)
  }

  def sizeEstimate() = ((count1 min count2) * averageIntersectionProportion).toInt

  def selectPage(count: Int, cursor: Cursor) = selectPageByDestinationId(count, cursor)

  def selectPageByDestinationId(count: Int, cursor: Cursor) = {
    if (count1 == 0 || count2 == 0) {
      new ResultWindow(List[(Long,Cursor)](), count, cursor)
    } else {
      val guessedPageSize = (count / averageIntersectionProportion).toInt
      val internalPageSize = guessedPageSize min intersectionPageSizeMax.toInt
      val timeout = intersectionTimeout.inMillis

      val now = System.currentTimeMillis
      var resultWindow = pageIntersection(smallerQuery, largerQuery, internalPageSize, count, cursor)
      while (resultWindow.page.size < count &&
             resultWindow.continueCursor != Cursor.End &&
             System.currentTimeMillis - now < timeout
      ) {
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
