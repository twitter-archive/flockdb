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

import scala.util.Sorting

class UnionQuery(query1: QueryTree, query2: QueryTree) extends ComplexQueryNode(query1, query2) {
  def sizeEstimate() = getSizeEstimates() map { case (count1, count2) => count1 max count2 }

  def selectPage(count: Int, cursor: Cursor) = selectPageByDestinationId(count, cursor)

  def selectPageByDestinationId(count: Int, cursor: Cursor) = time {
    val f1 = query1.selectPageByDestinationId(count, cursor)
    val f2 = query2.selectPageByDestinationId(count, cursor)

    for (result1 <- f1; result2 <- f2) yield result1.merge(result2)
  }

  def selectWhereIn(page: Seq[Long]) = time {
    val f1 = query1.selectWhereIn(page)
    val f2 = query2.selectWhereIn(page)

    for (page1 <- f1; page2 <- f2) yield {
      Sorting.stableSort((page1 ++ page2).toSet.toSeq)
    }
  }

  private def merge(page1: Seq[Long], page2: Seq[Long]): Seq[Long] = {
    Sorting.stableSort((Set(page1: _*) ++ Set(page2: _*)).toSeq)
  }

  override def toString =
    "<UnionQuery query1="+query1.toString+" query2="+query2.toString+duration.map(" time="+_.inMillis).mkString+">"
}
