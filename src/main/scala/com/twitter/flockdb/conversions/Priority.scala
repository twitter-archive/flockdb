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

import com.twitter.gizzard.thrift.conversions.Sequences._
import conversions.ExecuteOperation._
import com.twitter.flockdb

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
