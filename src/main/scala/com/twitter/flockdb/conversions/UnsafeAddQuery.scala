package com.twitter.flockdb.conversions

import com.twitter.util.Time
import com.twitter.util.TimeConversions._


object UnsafeAddQuery {
  class RichFlockUnsafeAddQuery(term: flockdb.UnsafeAddQuery) {
    def toThrift = new thrift.UnsafeAddQuery(term.sourceId, term.graphId, term.destinationId, term.createdAt.inMillis)
  }
  implicit def richFlockUnsafeAddQuery(term: flockdb.UnsafeAddQuery) = new RichFlockUnsafeAddQuery(term)

  class RichThriftUnsafeAddQuery(term: thrift.UnsafeAddQuery) {
    def fromThrift = new flockdb.UnsafeAddQuery(term.source_id, term.graph_id, term.destination_id, Time(term.created_at.millis))
  }
  implicit def richThriftUnsafeAddQuery(term: thrift.UnsafeAddQuery) = new RichThriftUnsafeAddQuery(term)
}
