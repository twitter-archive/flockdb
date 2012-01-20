/*
 * Copyright 2010 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.twitter.flockdb
package unit

import org.specs.mock.JMocker
import shards.Shard

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
          one(shard).count(sourceId, List(State.Normal))() willReturn 10
        }
        simpleQuery = new queries.SimpleQuery(shard, sourceId, List(State.Normal))
        simpleQuery.sizeEstimate() mustEqual 10
      }

      "when the state is abnormal" >> {
        expect {
          one(shard).count(sourceId, List(State.Removed))() willReturn 10
        }
        simpleQuery = new queries.SimpleQuery(shard, sourceId, List(State.Removed))
        simpleQuery.sizeEstimate() mustEqual 10
      }
    }

    "selectWhereIn" in {
      val page = List(1L, 2L, 3L, 4L)
      expect {
        one(shard).intersect(sourceId, List(State.Normal), page)() willReturn List(1L, 2L)
      }
      simpleQuery = new queries.SimpleQuery(shard, sourceId, List(State.Normal))
      simpleQuery.selectWhereIn(page)().toList mustEqual List(1L, 2L)
    }

    "selectPage" in {
      var edges = List[Long](101L, 103L, 104L, 107L, 108L)
      val cursor = Cursor(102L)
      val count = 5
      expect {
        allowing(shard).selectByPosition(sourceId, List(State.Normal), count, cursor)() willReturn new ResultWindow(Cursor.cursorZip(edges), Cursor.End, Cursor.End, count, cursor)
      }
      simpleQuery = new queries.SimpleQuery(shard, sourceId, List(State.Normal))
      simpleQuery.selectPage(count, cursor)().toTuple mustEqual (edges, Cursor.End, Cursor.End)
    }

    "selectPageByDestinationId" in {
      val edges = List[Long](101L, 103L, 104L, 107L, 108L)
      val cursor = Cursor(102L)
      val count = 5
      expect {
        allowing(shard).selectByDestinationId(sourceId, List(State.Normal), count, cursor)() willReturn new ResultWindow(Cursor.cursorZip(edges), Cursor.End, Cursor.End, count, cursor)
      }
      simpleQuery = new queries.SimpleQuery(shard, sourceId, List(State.Normal))
      simpleQuery.selectPageByDestinationId(count, cursor)().toTuple mustEqual (edges, Cursor.End, Cursor.End)
    }
  }
}
