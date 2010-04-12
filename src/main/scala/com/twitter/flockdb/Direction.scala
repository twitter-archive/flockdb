package com.twitter.flockdb

abstract sealed case class Direction(id: Int) {
  val opposite: Direction
}

object Direction {
  def apply(id: Int) = id match {
    case Forward.id => Forward
    case Backward.id => Backward
  }

  def apply(isForward: Boolean) = if (isForward) Forward else Backward

  case object Forward extends Direction(0) {
    val opposite = Direction.Backward
  }

  case object Backward extends Direction(1) {
    val opposite = Direction.Forward
  }

  val All = List(Forward, Backward)
}
