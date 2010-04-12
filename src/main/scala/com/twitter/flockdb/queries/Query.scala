package com.twitter.flockdb.queries

import com.twitter.results.{Cursor, Page, ResultWindow}


trait Query {
  def sizeEstimate(): Int
  def selectWhereIn(page: Seq[Long]): Seq[Long]
  def select(page: Page) = selectPage(page.count, page.cursor)
  def selectPageByDestinationId(count: Int, cursor: Cursor): ResultWindow[Long]

  protected def selectPage(count: Int, cursor: Cursor): ResultWindow[Long]
}
