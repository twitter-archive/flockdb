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

import com.twitter.util.Time
import com.twitter.conversions.time._
import org.specs.mock.{ClassMocker, JMocker}

object SelectCompilerSpec extends IntegrationSpecification with JMocker with ClassMocker {
  "SelectCompiler integration" should {
    val FOLLOWS = 1

    val alice = 1L
    val bob = 2L
    val carl = 3L
    val darcy = 4L

    def setup1() {
      execute(Select(alice, FOLLOWS, bob).add)
      execute(Select(alice, FOLLOWS, carl).add)
      execute(Select(alice, FOLLOWS, darcy).add)
      execute(Select(carl, FOLLOWS, bob).add)
      execute(Select(carl, FOLLOWS, darcy).add)

      flockService.contains(carl, FOLLOWS, darcy)() must eventually(beTrue)
    }

    def setup2() {
      for (i <- 1 until 11) execute(Select(alice, FOLLOWS, i).add)
      for (i <- 1 until 7) execute(Select(bob, FOLLOWS, i * 2).add)

      count(Select(alice, FOLLOWS, ())) must eventually(be_==(10))
      count(Select(bob, FOLLOWS, ())) must eventually(be_==(6))
    }

    "pagination" in {
      reset(config)
      setup1()

      val program = Select(alice, FOLLOWS, ()) intersect Select(carl, FOLLOWS, ())

      select(program, Page(1, Cursor.Start)) mustEqual ((List(darcy), Cursor(darcy), Cursor.End))
      select(program, new Page(1, Cursor(darcy))) mustEqual ((List(bob), Cursor.End, Cursor(-bob)))
      select(program, Page(2, Cursor.Start)) mustEqual ((List(darcy, bob), Cursor.End, Cursor.End))
    }

    "one list is empty" in {
      reset(config)
      setup2()

      val program = Select(alice, FOLLOWS, ()) intersect Select(carl, FOLLOWS, ())

      select(program, new Page(10, Cursor.Start)) mustEqual ((List(), Cursor.End, Cursor.End))
    }

    "difference" in {
      reset(config)
      setup2()

      val program = Select(alice, FOLLOWS, ()) difference Select(bob, FOLLOWS, ())

      select(program, new Page(10, Cursor.Start)) mustEqual ((List(9,7,5,3,1), Cursor.End, Cursor.End))
      select(program, new Page(2, Cursor.Start))  mustEqual ((List(9,7), Cursor(7), Cursor.End))
      select(program, new Page(2, Cursor(7)))     mustEqual ((List(5,3), Cursor(3), Cursor(-5)))
      select(program, new Page(2, Cursor(3)))     mustEqual ((List(1), Cursor.End, Cursor(-1)))
    }
  }
}
