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

import com.twitter.util.{Time, Duration}

trait Query {
  def sizeEstimate(): Int
  def selectWhereIn(page: Seq[Long]): Seq[Long]
  def select(page: Page) = selectPage(page.count, page.cursor)
  def selectPageByDestinationId(count: Int, cursor: Cursor): ResultWindow[Long]

  protected def selectPage(count: Int, cursor: Cursor): ResultWindow[Long]
}

trait Timed {
  var duration: Option[Duration] = None

  protected def time[A](f: => A) = { 
    val start = Time.now
    try {
      val rv = f
      duration = Some(Time.now - start)
      rv
    } catch {
      case e =>
        duration = Some(Time.now - start)
        throw e
    }
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
}

abstract case class SimpleQueryNode() extends QueryTree {
  def getComplexity(): Int = 0
  def getDepth(): Int = 0
}
