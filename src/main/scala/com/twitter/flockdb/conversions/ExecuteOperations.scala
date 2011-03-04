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

import scala.collection.JavaConversions._
import com.twitter.gizzard.thrift.conversions.Sequences._
import conversions.ExecuteOperation._
import conversions.Priority._


object ExecuteOperations {
  class RichFlockExecuteOperations(executeOperations: operations.ExecuteOperations) {
    def toThrift = {
      val rv =
        new thrift.ExecuteOperations(executeOperations.operations.map { _.toThrift },
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
