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

import scala.util.Sorting
import net.lag.configgy.Configgy

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
