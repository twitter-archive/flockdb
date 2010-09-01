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

import com.twitter.results.Cursor
import net.lag.configgy.Configgy


class DifferenceQuery(query1: Query, query2: Query, val userTimeoutMS: Int) extends Query {
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
