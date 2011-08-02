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

import com.twitter.util.Duration
import com.twitter.util.TimeConversions._


class DifferenceQuery(query1: QueryTree, query2: QueryTree, averageIntersectionProportion: Double, intersectionPageSizeMax: Int, intersectionTimeout: Duration) extends ComplexQueryNode(query1, query2) {
  def sizeEstimate = query1.sizeEstimate

  def selectPage(count: Int, cursor: Cursor) = selectPageByDestinationId(count, cursor)

  def selectPageByDestinationId(count: Int, cursor: Cursor) = time({
    val guessedPageSize = (count + count * averageIntersectionProportion).toInt
    val internalPageSize = guessedPageSize min intersectionPageSizeMax
    val timeout = intersectionTimeout.inMillis

    var resultWindow = pageDifference(internalPageSize, count, cursor)
    val now = System.currentTimeMillis
    while (resultWindow.page.size < count &&
           resultWindow.continueCursor != Cursor.End &&
           System.currentTimeMillis - now < timeout
    ) {
      resultWindow = resultWindow ++ pageDifference(internalPageSize, count, resultWindow.continueCursor)
    }
    resultWindow
  })

  def selectWhereIn(page: Seq[Long]) = time({
    val results = query1.selectWhereIn(page)
    val rejects = Set(query2.selectWhereIn(results): _*)
    results.filter { item => !rejects.contains(item) }
  })

  private def pageDifference(internalPageSize: Int, count: Int, cursor: Cursor) = {
    val results = query1.selectPageByDestinationId(internalPageSize, cursor)
    val rejects = query2.selectWhereIn(results.view)
    results -- rejects
  }

  override def toString =
    "<DifferenceQuery query1="+query1.toString+" query2="+query2.toString+duration.map(" time="+_.inMillis).mkString+">"
}
