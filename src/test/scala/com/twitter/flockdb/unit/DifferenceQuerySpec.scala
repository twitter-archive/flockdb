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

class DifferenceQuerySpec extends ConfiguredSpecification with JMocker {
  "DifferenceQuery" should {
    val query1 = new queries.SeqQuery(List(1,2,3,4,5,6,7,8,9,10,11,12))
    val query2 = new queries.SeqQuery(List(3,4,7,11))
    val queryConfig = config.intersectionQuery

    "sizeEstimate" in {
      val differenceQuery = queryConfig.difference(query1, query2)
      differenceQuery.sizeEstimate() mustEqual 12
    }

    "selectWhereIn" in {
      val differenceQuery = queryConfig.difference(query1, query2)
      differenceQuery.selectWhereIn(List(1, 2, 3, 4, 5, 11, 12, 13))().toList mustEqual List(12,5,2,1)
    }

    "selectPage" in {
      val differenceQuery = queryConfig.difference(query1, query2)

      differenceQuery.selectPage(5, Cursor.Start)().toTuple  mustEqual (List(12,10,9,8,6), Cursor(6), Cursor.End)
      differenceQuery.selectPage(10, Cursor(12L))().toTuple  mustEqual (List(10,9,8,6,5,2,1), Cursor.End, Cursor(-10))
      differenceQuery.selectPage(10, Cursor.Start)().toTuple mustEqual (List(12,10,9,8,6,5,2,1), Cursor.End, Cursor.End)
    }
  }
}
