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

object IntersectionQuerySpec extends ConfiguredSpecification with JMocker {
  "IntersectionQuery" should {
    val query1 = new queries.SeqQuery(List(1,2,3,4,5,6,7,8,9,10))
    val query2 = new queries.SeqQuery(List(1,2,3,4,11))
    val queryConfig = config.intersectionQuery

    "sizeEstimate" in {
      val intersectionQuery = queryConfig.intersect(query1, query2)
      intersectionQuery.sizeEstimate()() mustEqual (5 * queryConfig.averageIntersectionProportion).toInt
    }

    "selectWhereIn" in {
      val intersectionQuery = queryConfig.intersect(query1, query2)
      intersectionQuery.selectWhereIn(List(1, 2, 12, 13))() mustEqual List(2, 1)
    }

    "selectPage" in {
      val intersectionQuery = queryConfig.intersect(query1, query2)
      intersectionQuery.selectPage(5, Cursor.Start)().toTuple mustEqual (List(4, 3, 2, 1), Cursor.End, Cursor.End)
    }
  }
}
