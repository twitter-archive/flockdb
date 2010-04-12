package com.twitter.service.flock

abstract case class State(id: Int, name: String, ordinal: Int) {
  def max(s: State) = if (ordinal > s.ordinal) this else s
}

object State {
  def apply(id: Int) = id match {
    case Normal.id => Normal
    case Removed.id => Removed
    case Archived.id => Archived
    case Negative.id => Negative
  }

  case object Normal extends State(0, "Normal", 0)
  case object Negative extends State(3, "Negative", 1)
  case object Removed extends State(1, "Removed", 3)
  case object Archived extends State(2, "Archived", 2)
}
