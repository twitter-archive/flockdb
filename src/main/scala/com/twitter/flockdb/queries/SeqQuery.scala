package com.twitter.flockdb.queries

import scala.util.Sorting
import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.results.{Cursor, ResultWindow}


class SeqQuery(s: Seq[Long]) extends Query {
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
