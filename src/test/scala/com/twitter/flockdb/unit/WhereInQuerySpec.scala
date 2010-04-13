package com.twitter.flockdb.unit

import scala.collection.mutable
import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.results.Cursor
import org.specs.mock.JMocker
import org.specs.Specification
import conversions.Results._
import shards.Shard
import thrift.Results


object WhereInQuerySpec extends Specification with JMocker {
  "WhereInQuery" should {
    var shard: Shard = null
    var whereInQuery: queries.WhereInQuery = null
    val sourceId = 900
    val destinationIds = List(55L, 60L, 65L, 70L, 75L, 80L, 85L)

    doBefore {
      shard = mock[Shard]
    }

    "sizeEstimate" in {
      whereInQuery = new queries.WhereInQuery(shard, sourceId, List(State.Normal), destinationIds)
      whereInQuery.sizeEstimate() mustEqual destinationIds.size
    }

    "selectWhereIn" in {
      val page = List(65L, 63L, 60L)
      expect {
        one(shard).intersect(sourceId, List(State.Normal), List(60L, 65L)) willReturn List(60L)
      }
      whereInQuery = new queries.WhereInQuery(shard, sourceId, List(State.Normal), destinationIds)
      whereInQuery.selectWhereIn(page).toList mustEqual List(60L)
    }

    "selectPage" in {
      expect {
        allowing(shard).intersect(sourceId, List(State.Normal), destinationIds) willReturn List(85L, 75L, 65L, 55L)
      }

      whereInQuery = new queries.WhereInQuery(shard, sourceId, List(State.Normal), destinationIds)
      whereInQuery.selectPage(10, Cursor(90L)).toThrift mustEqual new Results(List[Long](85L, 75L, 65L, 55L).pack, Cursor.End.position, Cursor.End.position)
      whereInQuery.selectPage(10, Cursor(75L)).toThrift mustEqual new Results(List[Long](65L, 55L).pack, Cursor.End.position, -65L)
      whereInQuery.selectPage(2, Cursor(-65L)).toThrift mustEqual new Results(List[Long](85L, 75L).pack, 75L, Cursor.End.position)
    }
  }
}
