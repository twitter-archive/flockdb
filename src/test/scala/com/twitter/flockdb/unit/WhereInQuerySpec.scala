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

class WhereInQuerySpec extends ConfiguredSpecification with JMocker {
  "WhereInQuery" should {
    var shard: Shard = null
    val sourceId = 900
    val destinationIds = List(55L, 60L, 65L, 70L, 75L, 80L, 85L)

    doBefore {
      shard = mock[Shard]
    }

    "sizeEstimate" in {
      val whereInQuery = new queries.WhereInQuery(shard, sourceId, List(State.Normal), destinationIds)
      whereInQuery.sizeEstimate() mustEqual destinationIds.size
    }

    "selectWhereIn" in {
      val page = List(65L, 63L, 60L)
      expect {
        one(shard).intersect(sourceId, List(State.Normal), List(60L, 65L)) willReturn List(60L)
      }
      val whereInQuery = new queries.WhereInQuery(shard, sourceId, List(State.Normal), destinationIds)
      whereInQuery.selectWhereIn(page).toList mustEqual List(60L)
    }

    "selectPage" in {
      expect {
        allowing(shard).intersect(sourceId, List(State.Normal), destinationIds) willReturn List(85L, 75L, 65L, 55L)
      }

      val whereInQuery = new queries.WhereInQuery(shard, sourceId, List(State.Normal), destinationIds)

      whereInQuery.selectPage(10, Cursor(90L)).toTuple mustEqual (List(85L, 75L, 65L, 55L), Cursor.End, Cursor.End)
      whereInQuery.selectPage(10, Cursor(75L)).toTuple mustEqual (List(65L, 55L), Cursor.End, Cursor(-65L))
      whereInQuery.selectPage(2, Cursor(-65L)).toTuple mustEqual (List(85L, 75L), Cursor(75L), Cursor.End)
    }
  }
}
