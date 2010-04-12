package com.twitter.flockdb.conversions

import conversions.QueryTerm._
import flockdb.operations


object SelectOperation {
  class RichFlockSelectOperation(selectOperation: operations.SelectOperation) {
    def toThrift = {
      val thriftOp =
        new thrift.SelectOperation(thrift.SelectOperationType.findByValue(selectOperation.operationType.id))
      selectOperation.term.map { x => thriftOp.setTerm(x.toThrift) }
      thriftOp
    }
  }
  implicit def richFlockSelectOperation(selectOperation: operations.SelectOperation) =
    new RichFlockSelectOperation(selectOperation)

  class RichThriftSelectOperation(selectOperation: thrift.SelectOperation) {
    val term = if (selectOperation.isSetTerm) Some(selectOperation.term.fromThrift) else None
    def fromThrift =
      new operations.SelectOperation(operations.SelectOperationType(selectOperation.operation_type.getValue), term)
  }
  implicit def richThriftSelectOperation(selectOperation: thrift.SelectOperation) =
    new RichThriftSelectOperation(selectOperation)
}
