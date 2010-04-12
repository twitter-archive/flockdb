package com.twitter.service.flock.conversions

import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.results.ResultWindow


object Results {
  class RichResultWindowOfLongs(resultWindow: ResultWindow[Long]) {
    def toThrift = new thrift.Results(resultWindow.toList.pack, resultWindow.nextCursor.position,
                                      resultWindow.prevCursor.position)
  }
  implicit def richResultWindowOfLongs(resultWindow: ResultWindow[Long]) =
    new RichResultWindowOfLongs(resultWindow)
}
