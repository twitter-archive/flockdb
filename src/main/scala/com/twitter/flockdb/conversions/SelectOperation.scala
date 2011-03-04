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

package com.twitter.flockdb
package conversions

import conversions.QueryTerm._


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
