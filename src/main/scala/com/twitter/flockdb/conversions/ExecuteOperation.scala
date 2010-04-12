package com.twitter.flockdb.conversions

import conversions.QueryTerm._
import flockdb.operations


object ExecuteOperation {
  class RichFlockExecuteOperation(executeOperation: operations.ExecuteOperation) {
    def toThrift = {
      val thriftOp =
        new thrift.ExecuteOperation(thrift.ExecuteOperationType.findByValue(executeOperation.operationType.id),
                                    executeOperation.term.toThrift)
      executeOperation.position.map { thriftOp.setPosition(_) }
      thriftOp
    }
  }
  implicit def richFlockExecuteOperation(executeOperation: operations.ExecuteOperation) =
    new RichFlockExecuteOperation(executeOperation)

  class RichThriftExecuteOperation(executeOperation: thrift.ExecuteOperation) {
    def fromThrift = {
      val position =
        if (executeOperation.isSetPosition) Some(executeOperation.position) else None
      new operations.ExecuteOperation(operations.ExecuteOperationType(executeOperation.operation_type.getValue),
                                                                      executeOperation.term.fromThrift, position)
    }
  }
  implicit def richThriftExecuteOperation(executeOperation: thrift.ExecuteOperation) =
    new RichThriftExecuteOperation(executeOperation)
}
