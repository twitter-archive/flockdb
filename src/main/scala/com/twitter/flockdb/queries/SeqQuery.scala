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
import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.results.{Cursor, ResultWindow}


class SeqQuery(s: Seq[Long], val userTimeoutMS: Int) extends Query {
  val seq = sort(s)
  def sizeEstimate = seq.size
  def selectWhereIn(i: Seq[Long]) = sort(seq.toList intersect i.toList).toList
  protected def selectPage(count: Int, cursor: Cursor) = selectPageByDestinationId(count, cursor)
  def selectPageByDestinationId(count: Int, cursor: Cursor) = {
    val filtered = cursor match {
      case Cursor.Start => seq
      case Cursor.End => Nil
      case _ => seq.filter(_ <= cursor.position)
    }

    new ResultWindow(Cursor.cursorZip(filtered), count, cursor)
  }

  private def sort(s: Seq[Long]) = Sorting.stableSort(s, (x: Long, y: Long) => y < x)
}
