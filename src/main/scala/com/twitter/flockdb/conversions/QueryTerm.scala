package com.twitter.flockdb.conversions

import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.service.flock.State


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
      Some(term.destination_ids.toLongArray)
    } else {
      None
    }
    val stateIds = if (term.isSetState_ids && term.state_ids != null) {
      term.state_ids.toSeq.map { State(_) }
    } else {
      List[State]().toArray
    }
    def fromThrift = new flockdb.QueryTerm(term.source_id, term.graph_id, term.is_forward,
                                         destinationIds, stateIds)
  }
  implicit def richThriftQueryTerm(term: thrift.QueryTerm) = new RichThriftQueryTerm(term)
}
