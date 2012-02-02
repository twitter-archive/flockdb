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

package com.twitter.flockdb
package queries

import com.twitter.util.{Duration, Future}
import com.twitter.gizzard.Stats

class IntersectionQuery(query1: QueryTree, query2: QueryTree, averageIntersectionProportion: Double, intersectionPageSizeMax: Int, intersectionTimeout: Duration) extends ComplexQueryNode(query1, query2) {
  def sizeEstimate() = {
    getSizeEstimates() map { case (count1, count2) =>
      ((count1 min count2) * averageIntersectionProportion).toInt
    }
  }

  def selectPage(count: Int, cursor: Cursor) = selectPageByDestinationId(count, cursor)

  def selectPageByDestinationId(count: Int, cursor: Cursor) = time {
    getSizeEstimates() flatMap { case (count1, count2) =>
      if (count1 == 0 || count2 == 0) {
        Future(new ResultWindow(List[(Long,Cursor)](), count, cursor))
      } else {
        val guessedPageSize = (count / averageIntersectionProportion).toInt
        val internalPageSize = guessedPageSize min intersectionPageSizeMax.toInt
        val timeout = intersectionTimeout.inMillis
        val startTime = System.currentTimeMillis

        def loop(smaller: Query, larger: Query, currCursor: Cursor): Future[ResultWindow[Long]] = {
          pageIntersection(smaller, larger, internalPageSize, count, currCursor) flatMap { resultWindow =>
            if (resultWindow.page.size < count &&
                resultWindow.continueCursor != Cursor.End &&
                System.currentTimeMillis - startTime < timeout) {
              loop(smaller, larger, resultWindow.continueCursor) map { resultWindow ++ _ }
            } else {
              Future(resultWindow)
            }
          }
        }

        orderQueries() flatMap { case (smaller, larger) => loop(smaller, larger, cursor) }
      }
    }
  }

  def selectWhereIn(page: Seq[Long]) = time {
    orderQueries() flatMap { case (smaller, larger) =>
      smaller.selectWhereIn(page) flatMap { larger.selectWhereIn(_) }
    }
  }

  private def pageIntersection(smallerQuery: Query, largerQuery: Query, internalPageSize: Int, count: Int, cursor: Cursor) = {
    for {
      results <- smallerQuery.selectPageByDestinationId(internalPageSize, cursor)
      whereIn <- largerQuery.selectWhereIn(results.view)
    } yield {
      new ResultWindow(Cursor.cursorZip(whereIn), results.nextCursor, results.prevCursor, count, cursor)
    }
  }

  override def toString =
    "<IntersectionQuery query1="+query1.toString+" query2="+query2.toString+duration.map(" time="+_.inMillis).mkString+">"
}
