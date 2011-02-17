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
import com.twitter.flockdb
import Page._
import SelectOperation._


object SelectQuery {
  class RichFlockSelectQuery(query: flockdb.SelectQuery) {
    def toThrift = new thrift.SelectQuery(query.operations.map { _.toThrift }.toJavaList,
                                          query.page.toThrift)
  }
  implicit def richFlockSelectQuery(query: flockdb.SelectQuery) = new RichFlockSelectQuery(query)

  class RichThriftSelectQuery(query: thrift.SelectQuery) {
    def fromThrift = new flockdb.SelectQuery(query.operations.toSeq.map { _.fromThrift }.toList,
                                           query.page.fromThrift)
  }
  implicit def richThriftSelectQuery(query: thrift.SelectQuery) = new RichThriftSelectQuery(query)
}
