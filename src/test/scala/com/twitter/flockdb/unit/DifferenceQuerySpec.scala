package com.twitter.flockdb.unit

import scala.collection.mutable
import scala.util.Sorting
import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.results.Cursor
import org.specs.mock.JMocker
import conversions.Results._
import thrift.{Results, Page}


object DifferenceQuerySpec extends ConfiguredSpecification with JMocker {
  "DifferenceQuery" should {
    val query1 = new queries.SeqQuery(List(1,2,3,4,5,6,7,8,9,10,11,12))
    val query2 = new queries.SeqQuery(List(3,4,7,11))

    "sizeEstimate" in {
      val differenceQuery = new queries.DifferenceQuery(query1, query2)
      differenceQuery.sizeEstimate() mustEqual 12
    }

    "selectWhereIn" in {
      val differenceQuery = new queries.DifferenceQuery(query1, query2)
      differenceQuery.selectWhereIn(List(1, 2, 3, 4, 5, 11, 12, 13)).toList mustEqual List(12,5,2,1)
    }

    "selectPage" in {
      val differenceQuery = new queries.DifferenceQuery(query1, query2)
      differenceQuery.selectPage(5, Cursor.Start).toThrift mustEqual new Results(List[Long](12,10,9,8,6).pack, 6, Cursor.End.position)
      differenceQuery.selectPage(10, Cursor(12L)).toThrift mustEqual new Results(List[Long](10,9,8,6,5,2,1).pack, Cursor.End.position, -10)
      differenceQuery.selectPage(10, Cursor.Start).toThrift mustEqual new Results(List[Long](12,10,9,8,6,5,2,1).pack, Cursor.End.position, Cursor.End.position)
    }
  }
}
