package com.twitter.flockdb.conversions

import com.twitter.gizzard.thrift.conversions.Sequences._
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
