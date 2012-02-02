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

import com.twitter.querulous.evaluator.QueryEvaluatorFactory

object IntersectionSpec extends IntegrationSpecification {

  val FOLLOWS = 1

  val alice = 1L
  val bob = 2L
  val carl = 3L
  val darcy = 4L
  var queryEvaluatorFactories: List[QueryEvaluatorFactory] = null


  def intersectionOf(user1: Long, user2: Long, page: Page) = {
    select(Select(user1, FOLLOWS, ()) intersect Select(user2, FOLLOWS, ()), page)
  }

  def intersectAlot = {
    "intersection_for" in {
      "pagination" in {
        reset(config)
        execute(Select(alice, FOLLOWS, bob).add)
        execute(Select(alice, FOLLOWS, carl).add)
        execute(Select(alice, FOLLOWS, darcy).add)
        execute(Select(carl, FOLLOWS, bob).add)
        execute(Select(carl, FOLLOWS, darcy).add)

        flockService.contains(carl, FOLLOWS, darcy)() must eventually(beTrue)

        intersectionOf(alice, carl, new Page(1, Cursor.Start))  mustEqual ((List(darcy), Cursor(darcy), Cursor.End))
        intersectionOf(alice, carl, new Page(1, Cursor(darcy))) mustEqual ((List(bob), Cursor.End, Cursor(-bob)))
        intersectionOf(alice, carl, new Page(2, Cursor.Start))  mustEqual ((List(darcy, bob), Cursor.End, Cursor.End))
      }

      "one list is empty" in {
        reset(config)
        for (i <- 1 until 11) execute(Select(alice, FOLLOWS, i).add)
        count(Select(alice, FOLLOWS, ())) must eventually(be_==(10))

        intersectionOf(alice, carl, new Page(10, Cursor.Start)) mustEqual (Nil, Cursor.End, Cursor.End)
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
