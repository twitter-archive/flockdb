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

import com.twitter.xrayspecs.Time
import com.twitter.xrayspecs.TimeConversions._


object Edge {
  class RichFlockEdge(edge: flockdb.Edge) {
    def toThrift = new thrift.Edge(edge.sourceId, edge.destinationId, edge.position,
                                   edge.updatedAt.inSeconds,
                                   1, // deprecated, arbitrary value
                                   edge.state.id)
  }
  implicit def richFlockEdge(edge: flockdb.Edge) = new RichFlockEdge(edge)

  class RichThriftEdge(edge: thrift.Edge) {
    def fromThrift = new flockdb.Edge(edge.source_id, edge.destination_id, edge.position,
                                      Time(edge.updated_at.seconds),
                                      State(edge.state_id))
  }
  implicit def richThriftEdge(edge: thrift.Edge) = new RichThriftEdge(edge)
}
