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
import com.twitter.gizzard.thrift.conversions.Sequences._
import org.specs.mock.JMocker
import conversions.Results._
import thrift.Results

class UnionQuerySpec extends ConfiguredSpecification with JMocker {
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
