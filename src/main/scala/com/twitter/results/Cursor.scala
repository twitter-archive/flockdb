package com.twitter.results

object Cursor {
  def cursorZip(seq: Seq[Long]) = for (i <- seq) yield (i, Cursor(i))

  val End = new Cursor(0)
  val Start = new Cursor(-1)
}

case class Cursor(position: Long) extends Ordered[Cursor] {
  def compare(that: Cursor) = position.compare(that.position)
  def reverse = new Cursor(-position)
  def magnitude = new Cursor(Math.abs(position))
}
