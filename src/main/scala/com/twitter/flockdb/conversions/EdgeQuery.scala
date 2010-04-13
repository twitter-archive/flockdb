package com.twitter.flockdb.conversions

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
