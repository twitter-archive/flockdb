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

import scala.collection.JavaConversions._
import scala.collection.mutable
import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.util.Time
import com.twitter.util.TimeConversions._
import org.specs.mock.{ClassMocker, JMocker}
import thrift._

object SelectCompilerSpec extends IntegrationSpecification with JMocker with ClassMocker {
  "SelectCompiler integration" should {
    val FOLLOWS = 1

    val alice = 1L
    val bob = 2L
    val carl = 3L
    val darcy = 4L

    def setup1() {
      flockService.execute(Select(alice, FOLLOWS, bob).add.toThrift)
      flockService.execute(Select(alice, FOLLOWS, carl).add.toThrift)
      flockService.execute(Select(alice, FOLLOWS, darcy).add.toThrift)
      flockService.execute(Select(carl, FOLLOWS, bob).add.toThrift)
      flockService.execute(Select(carl, FOLLOWS, darcy).add.toThrift)
      flockService.contains(carl, FOLLOWS, darcy) must eventually(beTrue)
    }

    def setup2() {
      for (i <- 1 until 11) flockService.execute(Select(alice, FOLLOWS, i).add.toThrift)
      for (i <- 1 until 7) flockService.execute(Select(bob, FOLLOWS, i * 2).add.toThrift)
      flockService.count(Select(alice, FOLLOWS, ()).toThrift) must eventually(be_==(10))
      flockService.count(Select(bob, FOLLOWS, ()).toThrift) must eventually(be_==(6))
    }

    "pagination" in {
      reset(config)
      setup1()
      val term1 = new QueryTerm(alice, FOLLOWS, true)
      term1.setState_ids(List[Int](State.Normal.id))
      val term2 = new QueryTerm(carl, FOLLOWS, true)
      term2.setState_ids(List[Int](State.Normal.id))
      val op1 = new SelectOperation(SelectOperationType.SimpleQuery)
      op1.setTerm(term1)
      val op2 = new SelectOperation(SelectOperationType.SimpleQuery)
      op2.setTerm(term2)
      var program = op1 :: op2 ::
        new SelectOperation(SelectOperationType.Intersection) :: Nil

      var result = new Results(List[Long](darcy).pack, darcy, Cursor.End.position)
      flockService.select(program, new Page(1, Cursor.Start.position)) mustEqual result

      result = new Results(List[Long](bob).pack, Cursor.End.position, -bob)
      flockService.select(program, new Page(1, darcy)) mustEqual result

      result = new Results(List[Long](darcy, bob).pack, Cursor.End.position, Cursor.End.position)
      flockService.select(program, new Page(2, Cursor.Start.position)) mustEqual result
    }

    "one list is empty" in {
      reset(config)
      setup2()
      var result = new Results(List[Long]().pack, Cursor.End.position, Cursor.End.position)
      val term1 = new QueryTerm(alice, FOLLOWS, true)
      term1.setState_ids(List[Int](State.Normal.id))
      val term2 = new QueryTerm(carl, FOLLOWS, true)
      term2.setState_ids(List[Int](State.Normal.id))
      val op1 = new SelectOperation(SelectOperationType.SimpleQuery)
      op1.setTerm(term1)
      val op2 = new SelectOperation(SelectOperationType.SimpleQuery)
      op2.setTerm(term2)
      var program = op1 :: op2 ::
        new SelectOperation(SelectOperationType.Intersection) :: Nil
      flockService.select(program, new Page(10, Cursor.Start.position)) mustEqual result
    }

    "difference" in {
      reset(config)
      setup2()
      val term1 = new QueryTerm(alice, FOLLOWS, true)
      term1.setState_ids(List[Int](State.Normal.id))
      val term2 = new QueryTerm(bob, FOLLOWS, true)
      term2.setState_ids(List[Int](State.Normal.id))
      val op1 = new SelectOperation(SelectOperationType.SimpleQuery)
      op1.setTerm(term1)
      val op2 = new SelectOperation(SelectOperationType.SimpleQuery)
      op2.setTerm(term2)
      var program = op1 :: op2 ::
        new SelectOperation(SelectOperationType.Difference) :: Nil
      flockService.select(program, new Page(10, Cursor.Start.position)) mustEqual new Results(List[Long](9,7,5,3,1).pack, Cursor.End.position, Cursor.End.position)
      flockService.select(program, new Page(2, Cursor.Start.position)) mustEqual new Results(List[Long](9,7).pack, 7, Cursor.End.position)
      flockService.select(program, new Page(2, 7)) mustEqual new Results(List[Long](5,3).pack, 3, -5)
      flockService.select(program, new Page(2, 3)) mustEqual new Results(List[Long](1).pack, Cursor.End.position, -1)
    }
  }
}
