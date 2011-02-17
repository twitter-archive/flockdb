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
import com.twitter.flockdb

object QueryTerm {
  class RichFlockQueryTerm(term: flockdb.QueryTerm) {
    def toThrift = {
      val thriftTerm = new thrift.QueryTerm(term.sourceId, term.graphId, term.isForward)
      term.destinationIds.map { x => thriftTerm.setDestination_ids(x.pack) }
      thriftTerm.setState_ids(term.states.map { _.id }.toJavaList)
      thriftTerm
    }
  }
  implicit def richFlockQueryTerm(term: flockdb.QueryTerm) = new RichFlockQueryTerm(term)

  class RichThriftQueryTerm(term: thrift.QueryTerm) {
    val destinationIds = if (term.isSetDestination_ids && term.destination_ids != null) {
      Some(term.destination_ids.toLongArray.toSeq)
    } else {
      None
    }
    val stateIds = if (term.isSetState_ids && term.state_ids != null) {
      term.state_ids.toSeq.map { State(_) }
    } else {
      Seq[State]()
    }
    def fromThrift = new flockdb.QueryTerm(term.source_id, term.graph_id, term.is_forward,
                                         destinationIds, stateIds)
  }
  implicit def richThriftQueryTerm(term: thrift.QueryTerm) = new RichThriftQueryTerm(term)
}
