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

import com.twitter.flockdb
import Page._
import QueryTerm._

object EdgeQuery {
  class RichFlockEdgeQuery(query: flockdb.EdgeQuery) {
    def toThrift = new thrift.EdgeQuery(query.term.toThrift, query.page.toThrift)
  }
  implicit def richFlockEdgeQuery(query: flockdb.EdgeQuery) = new RichFlockEdgeQuery(query)

  class RichThriftEdgeQuery(query: thrift.EdgeQuery) {
    def fromThrift = new flockdb.EdgeQuery(query.term.fromThrift, query.page.fromThrift)
  }
  implicit def richThriftEdgeQuery(query: thrift.EdgeQuery) = new RichThriftEdgeQuery(query)
}
