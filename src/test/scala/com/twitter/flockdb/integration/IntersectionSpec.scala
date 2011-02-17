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
package integration

import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.querulous.evaluator.QueryEvaluatorFactory
import thrift.{Page, QueryTerm, Results, SelectOperation, SelectOperationType}

object IntersectionSpec extends IntegrationSpecification {

  val FOLLOWS = 1

  val alice = 1L
  val bob = 2L
  val carl = 3L
  val darcy = 4L
  var queryEvaluatorFactories: List[QueryEvaluatorFactory] = null


  def intersection_of(user1: Long, user2: Long, page: Page) = {
    val op1 = new SelectOperation(SelectOperationType.SimpleQuery)
    op1.setTerm(new QueryTerm(user1, FOLLOWS, true))
    val op2 = new SelectOperation(SelectOperationType.SimpleQuery)
    op2.setTerm(new QueryTerm(user2, FOLLOWS, true))
    flock.select(List[SelectOperation](
      op1,
      op2,
      new SelectOperation(SelectOperationType.Intersection)).toJavaList, page)
  }

  def intersectAlot = {
    "intersection_for" in {
      "pagination" in {
        reset(config)
        flock.execute(Select(alice, FOLLOWS, bob).add.toThrift)
        flock.execute(Select(alice, FOLLOWS, carl).add.toThrift)
        flock.execute(Select(alice, FOLLOWS, darcy).add.toThrift)
        flock.execute(Select(carl, FOLLOWS, bob).add.toThrift)
        flock.execute(Select(carl, FOLLOWS, darcy).add.toThrift)
        flock.contains(carl, FOLLOWS, darcy) must eventually(beTrue)

        var result = new Results(List[Long](darcy).pack, darcy, Cursor.End.position)
        intersection_of(alice, carl, new Page(1, Cursor.Start.position)) mustEqual result

        result = new Results(List[Long](bob).pack, Cursor.End.position, -bob)
        intersection_of(alice, carl, new Page(1, darcy)) mustEqual result

        result = new Results(List[Long](darcy, bob).pack, Cursor.End.position, Cursor.End.position)
        intersection_of(alice, carl, new Page(2, Cursor.Start.position)) mustEqual result
      }

      "one list is empty" in {
        reset(config)
        for (i <- 1 until 11) flock.execute(Select(alice, FOLLOWS, i).add.toThrift)
        flock.count(Select(alice, FOLLOWS, ()).toThrift) must eventually(be_==(10))

        var result = new Results(List[Long]().pack, Cursor.End.position, Cursor.End.position)
        intersection_of(alice, carl, new Page(10, Cursor.Start.position)) mustEqual result
      }
    }
  }

  "Intersection" should {
    "with a large intersection" >>  {
      config.intersectionQuery.intersectionPageSizeMax = 1

      intersectAlot
    }

    "with a small intersection" >> {
      config.intersectionQuery.intersectionPageSizeMax = Integer.MAX_VALUE - 1

      intersectAlot
    }
  }
}
