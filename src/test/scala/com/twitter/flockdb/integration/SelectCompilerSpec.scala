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

package com.twitter.flockdb.integration

import scala.collection.mutable
import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.results.Cursor
import com.twitter.xrayspecs.Time
import com.twitter.xrayspecs.TimeConversions._
import org.specs.mock.{ClassMocker, JMocker}
import test.{EdgesDatabase, StaticEdges}
import thrift._


object SelectCompilerSpec extends ConfiguredSpecification with EdgesDatabase with JMocker with ClassMocker {
  val poolConfig = config.configMap("db.connection_pool")

  import StaticEdges._

  "SelectCompiler integration" should {
    val FOLLOWS = 1

    val alice = 1L
    val bob = 2L
    val carl = 3L
    val darcy = 4L

    materialize(config.configMap("edges.nameservers"))

    doBefore {
      reset(flock)
    }

    def setup1() {
      flock.execute(Select(alice, FOLLOWS, bob).add.toThrift)
      flock.execute(Select(alice, FOLLOWS, carl).add.toThrift)
      flock.execute(Select(alice, FOLLOWS, darcy).add.toThrift)
      flock.execute(Select(carl, FOLLOWS, bob).add.toThrift)
      flock.execute(Select(carl, FOLLOWS, darcy).add.toThrift)
      flock.contains(carl, FOLLOWS, darcy) must eventually(beTrue)
    }

    def setup2() {
      for (i <- 1 until 11) flock.execute(Select(alice, FOLLOWS, i).add.toThrift)
      for (i <- 1 until 7) flock.execute(Select(bob, FOLLOWS, i * 2).add.toThrift)
      flock.count(Select(alice, FOLLOWS, ()).toThrift) must eventually(be_==(10))
      flock.count(Select(bob, FOLLOWS, ()).toThrift) must eventually(be_==(6))
    }

    "pagination" in {
      setup1()
      val term1 = new QueryTerm(alice, FOLLOWS, true)
      term1.setState_ids(List[Int](State.Normal.id).toJavaList)
      val term2 = new QueryTerm(carl, FOLLOWS, true)
      term2.setState_ids(List[Int](State.Normal.id).toJavaList)
      val op1 = new SelectOperation(SelectOperationType.SimpleQuery)
      op1.setTerm(term1)
      val op2 = new SelectOperation(SelectOperationType.SimpleQuery)
      op2.setTerm(term2)
      var program = op1 :: op2 ::
        new SelectOperation(SelectOperationType.Intersection) :: Nil

      var result = new Results(List[Long](darcy).pack, darcy, Cursor.End.position)
      flock.select(program.toJavaList, new Page(1, Cursor.Start.position)) mustEqual result

      result = new Results(List[Long](bob).pack, Cursor.End.position, -bob)
      flock.select(program.toJavaList, new Page(1, darcy)) mustEqual result

      result = new Results(List[Long](darcy, bob).pack, Cursor.End.position, Cursor.End.position)
      flock.select(program.toJavaList, new Page(2, Cursor.Start.position)) mustEqual result
    }

    "one list is empty" in {
      setup2()
      var result = new Results(List[Long]().pack, Cursor.End.position, Cursor.End.position)
      val term1 = new QueryTerm(alice, FOLLOWS, true)
      term1.setState_ids(List[Int](State.Normal.id).toJavaList)
      val term2 = new QueryTerm(carl, FOLLOWS, true)
      term2.setState_ids(List[Int](State.Normal.id).toJavaList)
      val op1 = new SelectOperation(SelectOperationType.SimpleQuery)
      op1.setTerm(term1)
      val op2 = new SelectOperation(SelectOperationType.SimpleQuery)
      op2.setTerm(term2)
      var program = op1 :: op2 ::
        new SelectOperation(SelectOperationType.Intersection) :: Nil
      flock.select(program.toJavaList, new Page(10, Cursor.Start.position)) mustEqual result
    }

    "difference" in {
      setup2()
      val term1 = new QueryTerm(alice, FOLLOWS, true)
      term1.setState_ids(List[Int](State.Normal.id).toJavaList)
      val term2 = new QueryTerm(bob, FOLLOWS, true)
      term2.setState_ids(List[Int](State.Normal.id).toJavaList)
      val op1 = new SelectOperation(SelectOperationType.SimpleQuery)
      op1.setTerm(term1)
      val op2 = new SelectOperation(SelectOperationType.SimpleQuery)
      op2.setTerm(term2)
      var program = op1 :: op2 ::
        new SelectOperation(SelectOperationType.Difference) :: Nil
      flock.select(program.toJavaList, new Page(10, Cursor.Start.position)) mustEqual new Results(List[Long](9,7,5,3,1).pack, Cursor.End.position, Cursor.End.position)
      flock.select(program.toJavaList, new Page(2, Cursor.Start.position)) mustEqual new Results(List[Long](9,7).pack, 7, Cursor.End.position)
      flock.select(program.toJavaList, new Page(2, 7)) mustEqual new Results(List[Long](5,3).pack, 3, -5)
      flock.select(program.toJavaList, new Page(2, 3)) mustEqual new Results(List[Long](1).pack, Cursor.End.position, -1)
    }
  }
}
