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

import com.twitter.util.{Time, Duration, Future}

trait Query {
  def sizeEstimate(): Future[Int]
  def selectWhereIn(page: Seq[Long]): Future[Seq[Long]]
  def select(page: Page) = selectPage(page.count, page.cursor)
  def selectPageByDestinationId(count: Int, cursor: Cursor): Future[ResultWindow[Long]]

  protected def selectPage(count: Int, cursor: Cursor): Future[ResultWindow[Long]]
}

trait Timed {
  var duration: Option[Duration] = None

  protected def time[A](f: => Future[A]): Future[A] = {
    val start = Time.now
    f map { rv => duration = Some(Time.now - start); rv }
  }
}

sealed abstract class QueryTree extends Query with Timed {
  def getComplexity(): Int
  def getDepth(): Int
}

abstract case class ComplexQueryNode(left: QueryTree, right: QueryTree) extends QueryTree {
  val complexity = (left.getComplexity() + right.getComplexity()) + 1
  val depth = (left.getDepth() max right.getDepth) + 1
  def getComplexity(): Int = complexity
  def getDepth(): Int = depth

  def getSizeEstimates() = {
    val f1 = left.sizeEstimate
    val f2 = right.sizeEstimate
    for (count1 <- f1; count2 <- f2) yield (count1, count2)
  }

  def orderQueries() = {
    getSizeEstimates() map { case (count1, count2) =>
      if (count1 < count2) {
        (left, right)
      } else {
        (right, left)
      }
    }
  }

}

abstract case class SimpleQueryNode() extends QueryTree {
  def getComplexity(): Int = 0
  def getDepth(): Int = 0
}
