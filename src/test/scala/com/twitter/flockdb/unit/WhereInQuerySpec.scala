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

package com.twitter.flockdb.unit

import scala.collection.mutable
import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.results.Cursor
import org.specs.mock.JMocker
import conversions.Results._
import shards.Shard
import thrift.Results


object WhereInQuerySpec extends ConfiguredSpecification with JMocker {
  "WhereInQuery" should {
    var shard: Shard = null
    var whereInQuery: queries.WhereInQuery = null
    val sourceId = 900
    val destinationIds = List(55L, 60L, 65L, 70L, 75L, 80L, 85L)
    val timeout = 0

    doBefore {
      shard = mock[Shard]
    }

    "sizeEstimate" in {
      whereInQuery = new queries.WhereInQuery(shard, sourceId, List(State.Normal), destinationIds, timeout)
      whereInQuery.sizeEstimate() mustEqual destinationIds.size
    }

    "selectWhereIn" in {
      val page = List(65L, 63L, 60L)
      expect {
        one(shard).intersect(sourceId, List(State.Normal), List(60L, 65L)) willReturn List(60L)
      }
      whereInQuery = new queries.WhereInQuery(shard, sourceId, List(State.Normal), destinationIds, timeout)
      whereInQuery.selectWhereIn(page).toList mustEqual List(60L)
    }

    "selectPage" in {
      expect {
        allowing(shard).intersect(sourceId, List(State.Normal), destinationIds) willReturn List(85L, 75L, 65L, 55L)
      }

      whereInQuery = new queries.WhereInQuery(shard, sourceId, List(State.Normal), destinationIds, timeout)
      whereInQuery.selectPage(10, Cursor(90L)).toThrift mustEqual new Results(List[Long](85L, 75L, 65L, 55L).pack, Cursor.End.position, Cursor.End.position)
      whereInQuery.selectPage(10, Cursor(75L)).toThrift mustEqual new Results(List[Long](65L, 55L).pack, Cursor.End.position, -65L)
      whereInQuery.selectPage(2, Cursor(-65L)).toThrift mustEqual new Results(List[Long](85L, 75L).pack, 75L, Cursor.End.position)
    }
  }
}
