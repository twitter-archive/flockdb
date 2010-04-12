package com.twitter.flockdb.operations


object ExecuteOperationType extends Enumeration {
  val Add = Value(1)
  val Remove = Value(2)
  val Archive = Value(3)
  val Negate = Value(4)
}
