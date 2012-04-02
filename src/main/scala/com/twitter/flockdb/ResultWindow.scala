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

import scala.util.Sorting
import com.twitter.util.Time
import com.twitter.util.TimeConversions._


case class ResultWindowRow[T](id: T, cursor: Cursor) extends Ordered[ResultWindowRow[T]] {
  def compare(that: ResultWindowRow[T]) = that.cursor.compare(cursor)
}

class ResultWindowRows[T](data: Seq[ResultWindowRow[T]]) extends Seq[ResultWindowRow[T]] {
  def length = data.length
  def apply(i: Int) = data(i)
  def iterator = data.iterator
}

class ResultWindow[T](val data: ResultWindowRows[T], val inNextCursor: Cursor, val inPrevCursor: Cursor, val count: Int, val cursor: Cursor) extends Seq[T] {
  def this(data: Seq[(T, Cursor)], inNextCursor: Cursor, inPrevCursor: Cursor, count: Int, cursor: Cursor) =
    this(new ResultWindowRows(data.map { datum => ResultWindowRow(datum._1, datum._2) }), inNextCursor, inPrevCursor, count, cursor)
  def this(data: Seq[(T, Cursor)], count: Int, cursor: Cursor) =
    this(data, Cursor.End, Cursor.End, count, cursor)
  def this() =
    this(List[(T, Cursor)](), 0, Cursor.End)

  var page: Seq[ResultWindowRow[T]] = data
  var nextChanged, prevChanged = false
  if (cursor < Cursor.Start) {
    page = data.takeWhile(_.cursor > cursor.magnitude)
    nextChanged = page.size < data.size
    prevChanged = page.size > count
    page = page.drop(page.size - count)
  } else if (cursor == Cursor.Start) {
    nextChanged = page.size > count
    page = page.take(count)
  } else {
    page = data.dropWhile(_.cursor >= cursor)
    nextChanged = page.size > count
    prevChanged = page.size < data.size
    page = page.take(count)
  }
  val nextCursor = if (nextChanged && !page.isEmpty) page(page.size - 1).cursor else inNextCursor
  val prevCursor = if (prevChanged && !page.isEmpty) page(0).cursor.reverse else inPrevCursor

  def ++(other: ResultWindow[T]) = concat(other)

  def concat(other: ResultWindow[T], newCount: Int = count) = {
    if (cursor < Cursor.Start) {
      new ResultWindow(new ResultWindowRows(other.page ++ page), nextCursor, other.prevCursor, newCount, cursor)
    } else {
      new ResultWindow(new ResultWindowRows(page ++ other.page), other.nextCursor, prevCursor, newCount, cursor)
    }
  }


  def merge(other: ResultWindow[T]) = {
    val newPage = Sorting.stableSort((Set((page ++ other.page): _*)).toSeq)
    val newNextCursor = if (nextCursor == Cursor.End && other.nextCursor == Cursor.End) Cursor.End else newPage(newPage.size - 1).cursor
    val newPrevCursor = if (prevCursor == Cursor.End && other.prevCursor == Cursor.End) Cursor.End else newPage(0).cursor.reverse
    new ResultWindow(new ResultWindowRows(newPage), newNextCursor, newPrevCursor, count, cursor)
  }

  def --(values: Seq[T]) = diff(values)

  def diff(values: Seq[T], newCount: Int = count) = {
    val rejects = Set(values: _*)
    val newPage = page.filter { row => !rejects.contains(row.id) }
    val newNextCursor = if (nextCursor == Cursor.End || newPage.size == 0) Cursor.End else newPage(newPage.size - 1).cursor
    val newPrevCursor = if (prevCursor == Cursor.End || newPage.size == 0) Cursor.End else newPage(0).cursor.reverse
    new ResultWindow(new ResultWindowRows(newPage), newNextCursor, newPrevCursor, newCount, cursor)
  }

  def length = page.length
  def apply(i: Int) = page(i).id
  def iterator = page.view.map(_.id).iterator
  def continueCursor = if (cursor < Cursor.Start) prevCursor else nextCursor
  override def headOption = page.headOption.map { _.id }

  override def toString = (iterator.toList, nextCursor, prevCursor, count, cursor).toString

  override def equals(that: Any) = that match {
    case that: ResultWindow[_] => iterator.toList == that.iterator.toList && nextCursor == that.nextCursor && prevCursor == that.prevCursor && cursor == that.cursor
    case _ => false
  }

  // convenience method that makes for easier matching in tests
  def toTuple = (iterator.toList, nextCursor, prevCursor)
}
