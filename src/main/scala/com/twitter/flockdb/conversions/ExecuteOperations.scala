package com.twitter.flockdb.conversions

import com.twitter.gizzard.thrift.conversions.Sequences._
import conversions.ExecuteOperation._
import conversions.Priority._


object ExecuteOperations {
  class RichFlockExecuteOperations(executeOperations: operations.ExecuteOperations) {
    def toThrift = {
      val rv =
        new thrift.ExecuteOperations(executeOperations.operations.map { _.toThrift }.toJavaList,
                                     executeOperations.priority.toThrift)
      executeOperations.executeAt.map { x => rv.setExecute_at(x) }
      rv
    }
  }
  implicit def richFlockExecuteOperations(executeOperations: operations.ExecuteOperations) =
    new RichFlockExecuteOperations(executeOperations)

  class RichThriftExecuteOperations(executeOperations: thrift.ExecuteOperations) {
    def fromThrift = {
      val executeAt = if (executeOperations.isSetExecute_at) Some(executeOperations.execute_at) else None
      new operations.ExecuteOperations(executeOperations.operations.toSeq.map { _.fromThrift },
                                  executeAt, executeOperations.priority.fromThrift)
    }
  }
  implicit def richThriftExecuteOperations(query: thrift.ExecuteOperations) =
    new RichThriftExecuteOperations(query)
}
