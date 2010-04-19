package com.twitter.flockdb.unit

import scala.collection.mutable
import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.results.Cursor
import org.specs.mock.JMocker
import conversions.Results._
import thrift.Results


object UnionQuerySpec extends ConfiguredSpecification with JMocker {
  "UnionQuery" should {
    val query1 = new queries.SeqQuery(List(1,2,3,4,5,6,7,8,9,10))
    val query2 = new queries.SeqQuery(List(1,2,3,4,11))

    "sizeEstimate" in {
      val unionQuery = new queries.UnionQuery(query1, query2)
      unionQuery.sizeEstimate mustEqual 10
    }

    "selectWhereIn" in {
      val unionQuery = new queries.UnionQuery(query1, query2)
      unionQuery.selectWhereIn(List[Long](1, 2, 3, 12)).toList mustEqual List[Long](1, 2, 3)
    }

    "selectPage" in {
      val unionQuery = new queries.UnionQuery(query1, query2)
      unionQuery.selectPage(10, Cursor(9)).toThrift mustEqual new Results(List[Long](8,7,6,5,4,3,2,1).pack, Cursor.End.position, -8)
    }
  }
}
