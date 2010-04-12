package com.twitter.flockdb.conversions

import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.results.ResultWindow
import conversions.Edge._


object EdgeResults {
  class RichResultWindowOfEdges(resultWindow: ResultWindow[Edge]) {
    def toEdgeResults = new thrift.EdgeResults(resultWindow.map { _.toThrift }.toJavaList,
                                               resultWindow.nextCursor.position, resultWindow.prevCursor.position)
  }
  implicit def richResultWindowOfEdges(resultWindow: ResultWindow[Edge]) =
    new RichResultWindowOfEdges(resultWindow)
}
