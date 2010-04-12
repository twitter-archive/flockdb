package com.twitter.service.flock.conversions

import com.twitter.results


object Page {
  class RichFlockPage(page: results.Page) {
    def toThrift = new thrift.Page(page.count, page.cursor.position)
  }
  implicit def richFlockPage(page: results.Page) = new RichFlockPage(page)

  class RichThriftPage(page: thrift.Page) {
    def fromThrift = new results.Page(page.count, results.Cursor(page.cursor))
  }
  implicit def richThriftPage(page: thrift.Page) = new RichThriftPage(page)
}
