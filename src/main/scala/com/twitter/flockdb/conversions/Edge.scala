package com.twitter.flockdb.conversions

import com.twitter.xrayspecs.Time
import com.twitter.xrayspecs.TimeConversions._


object Edge {
  class RichFlockEdge(edge: flockdb.Edge) {
    def toThrift = new thrift.Edge(edge.sourceId, edge.destinationId, edge.position,
                                   edge.updatedAt.inSeconds, edge.count, edge.state.id)
  }
  implicit def richFlockEdge(edge: flockdb.Edge) = new RichFlockEdge(edge)

  class RichThriftEdge(edge: thrift.Edge) {
    def fromThrift = new flockdb.Edge(edge.source_id, edge.destination_id, edge.position,
                                      Time(edge.updated_at.seconds), edge.count, State(edge.state_id))
  }
  implicit def richThriftEdge(edge: thrift.Edge) = new RichThriftEdge(edge)
}
