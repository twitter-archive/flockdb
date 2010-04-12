package com.twitter.flockdb.operations


object SelectOperationType extends Enumeration {
  val SimpleQuery = Value(1)
  val Intersection = Value(2)
  val Union = Value(3)
  val Difference = Value(4)
}
