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

import scala.collection.mutable
import scala.util.Sorting
import com.twitter.gizzard.thrift.conversions.Sequences._
import org.specs.mock.JMocker
import conversions.Results._
import thrift.{Results, Page}

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
      differenceQuery.selectWhereIn(List(1, 2, 3, 4, 5, 11, 12, 13)).toList mustEqual List(12,5,2,1)
    }
/*
    "selectPage" in {
      val differenceQuery = queryConfig.difference(query1, query2)
      differenceQuery.selectPage(5, Cursor.Start).toThrift mustEqual new Results(List[Long](12,10,9,8,6).pack, 6, Cursor.End.position)
      differenceQuery.selectPage(10, Cursor(12L)).toThrift mustEqual new Results(List[Long](10,9,8,6,5,2,1).pack, Cursor.End.position, -10)
      differenceQuery.selectPage(10, Cursor.Start).toThrift mustEqual new Results(List[Long](12,10,9,8,6,5,2,1).pack, Cursor.End.position, Cursor.End.position)
    }
*/
  }
}
