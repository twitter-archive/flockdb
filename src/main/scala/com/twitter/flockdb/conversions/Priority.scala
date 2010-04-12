package com.twitter.flockdb.conversions

import com.twitter.gizzard.thrift.conversions.Sequences._
import conversions.ExecuteOperation._


object Priority {
  class RichFlockPriority(priority: flockdb.Priority.Value) {
    def toThrift = priority match {
      case flockdb.Priority.High => thrift.Priority.High
      case flockdb.Priority.Medium => thrift.Priority.Medium
      case flockdb.Priority.Low => thrift.Priority.Low
    }
  }
  implicit def richFlockPriority(priority: flockdb.Priority.Value) = new RichFlockPriority(priority)

  class RichThriftPriority(priority: thrift.Priority) {
    def fromThrift = priority match {
      case thrift.Priority.High => flockdb.Priority.High
      case thrift.Priority.Medium => flockdb.Priority.Medium
      case thrift.Priority.Low => flockdb.Priority.Low
    }
  }
  implicit def richThriftPriority(priority: thrift.Priority) = new RichThriftPriority(priority)
}
