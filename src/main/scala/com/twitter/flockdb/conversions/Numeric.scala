package com.twitter.flockdb.conversions

object Numeric {
  class RichAnyVal(wrapped: AnyVal) {
    def toLong = {
      wrapped match {
        case i: Int => i.toLong
        case n: Long => n
      }
    }

    def toInt = {
      wrapped match {
        case i: Int => i
        case n: Long => n.toInt
      }
    }
  }

  implicit def anyValToRichAnyVal(v: AnyVal) = new RichAnyVal(v)
}
