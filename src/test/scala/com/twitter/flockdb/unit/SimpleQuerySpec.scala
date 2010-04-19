package com.twitter.flockdb.unit

import scala.collection.mutable
import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.results.{Cursor, ResultWindow}
import org.specs.mock.JMocker
import conversions.Results._
import shards.Shard
import thrift.Results


object SimpleQuerySpec extends ConfiguredSpecification with JMocker {
  "SimpleQuery" should {
    var shard: Shard = null
    var simpleQuery: queries.SimpleQuery = null
    val sourceId = 900

    doBefore {
      shard = mock[Shard]
    }

    "sizeEstimate" in {
      "when the state is normal" >> {
        expect {
          one(shard).count(sourceId, List(State.Normal)) willReturn 10
        }
        simpleQuery = new queries.SimpleQuery(shard, sourceId, List(State.Normal))
        simpleQuery.sizeEstimate() mustEqual 10
      }

      "when the state is abnormal" >> {
        expect {
          one(shard).count(sourceId, List(State.Removed)) willReturn 10
        }
        simpleQuery = new queries.SimpleQuery(shard, sourceId, List(State.Removed))
        simpleQuery.sizeEstimate() mustEqual 10
      }
    }

    "selectWhereIn" in {
      val page = List(1L, 2L, 3L, 4L)
      expect {
        one(shard).intersect(sourceId, List(State.Normal), page) willReturn List(1L, 2L)
      }
      simpleQuery = new queries.SimpleQuery(shard, sourceId, List(State.Normal))
      simpleQuery.selectWhereIn(page).toList mustEqual List(1L, 2L)
    }

    "selectPage" in {
      var edges = List[Long](101L, 103L, 104L, 107L, 108L)
      val cursor = Cursor(102L)
      val count = 5
      expect {
        allowing(shard).selectByPosition(sourceId, List(State.Normal), count, cursor) willReturn new ResultWindow(Cursor.cursorZip(edges), Cursor.End, Cursor.End, count, cursor)
      }
      simpleQuery = new queries.SimpleQuery(shard, sourceId, List(State.Normal))
      simpleQuery.selectPage(count, cursor).toThrift mustEqual new Results(edges.pack, Cursor.End.position, Cursor.End.position)
    }

    "selectPageByDestinationId" in {
      val edges = List[Long](101L, 103L, 104L, 107L, 108L)
      val cursor = Cursor(102L)
      val count = 5
      expect {
        allowing(shard).selectByDestinationId(sourceId, List(State.Normal), count, cursor) willReturn new ResultWindow(Cursor.cursorZip(edges), Cursor.End, Cursor.End, count, cursor)
      }
      simpleQuery = new queries.SimpleQuery(shard, sourceId, List(State.Normal))
      simpleQuery.selectPageByDestinationId(count, cursor).toThrift mustEqual new Results(edges.pack, Cursor.End.position, Cursor.End.position)
    }
  }
}
