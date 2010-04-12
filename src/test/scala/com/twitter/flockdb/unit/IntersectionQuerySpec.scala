package com.twitter.flockdb.unit

import scala.util.Sorting
import scala.collection.mutable
import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.results.Cursor
import com.twitter.service.flock.conversions.Results._
import com.twitter.service.flock.thrift.{Results, Page}
import net.lag.configgy.Configgy
import org.specs.mock.JMocker
import org.specs.Specification


object IntersectionQuerySpec extends Specification with JMocker {
  "IntersectionQuery" should {
    val query1 = new queries.SeqQuery(List(1,2,3,4,5,6,7,8,9,10))
    val query2 = new queries.SeqQuery(List(1,2,3,4,11))

    doBefore {
      Configgy.config("edges.average_intersection_proportion") = "1.0"
    }

    "sizeEstimate" in {
      val intersectionQuery = new queries.IntersectionQuery(query1, query2)
      intersectionQuery.sizeEstimate() mustEqual 5
    }

    "selectWhereIn" in {
      val intersectionQuery = new queries.IntersectionQuery(query1, query2)
      intersectionQuery.selectWhereIn(List(1, 2, 12, 13)) mustEqual List(2, 1)
    }

    "selectPage" in {
      val intersectionQuery = new queries.IntersectionQuery(query1, query2)
      intersectionQuery.selectPage(5, Cursor.Start).toThrift mustEqual new Results(List[Long](4, 3, 2, 1).pack, Cursor.End.position, Cursor.End.position)
    }
  }
}
