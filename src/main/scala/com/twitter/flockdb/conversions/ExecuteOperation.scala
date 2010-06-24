/*
 * Copyright 2010 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
