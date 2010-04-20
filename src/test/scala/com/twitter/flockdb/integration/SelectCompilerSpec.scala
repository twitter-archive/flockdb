package com.twitter.flockdb.integration

import scala.collection.mutable
import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.results.Cursor
import com.twitter.xrayspecs.{Time, Eventually}
import com.twitter.xrayspecs.TimeConversions._
import org.specs.mock.{ClassMocker, JMocker}
import test.{EdgesDatabase, StaticEdges}
import thrift._


object SelectCompilerSpec extends ConfiguredSpecification with Eventually with EdgesDatabase with JMocker with ClassMocker {
  val poolConfig = config.configMap("db.connection_pool")

  import StaticEdges._

  "SelectCompiler integration" should {
    val FOLLOWS = 1

    val alice = 1L
    val bob = 2L
    val carl = 3L
    val darcy = 4L

    doBefore {
      reset(edges)
    }

    def setup1() {
      edges.execute(Select(alice, FOLLOWS, bob).add.toThrift)
      edges.execute(Select(alice, FOLLOWS, carl).add.toThrift)
      edges.execute(Select(alice, FOLLOWS, darcy).add.toThrift)
      edges.execute(Select(carl, FOLLOWS, bob).add.toThrift)
      edges.execute(Select(carl, FOLLOWS, darcy).add.toThrift)
      edges.contains(carl, FOLLOWS, darcy) must eventually(beTrue)
    }

    def setup2() {
      for (i <- 1 until 11) edges.execute(Select(alice, FOLLOWS, i).add.toThrift)
      for (i <- 1 until 7) edges.execute(Select(bob, FOLLOWS, i * 2).add.toThrift)
      edges.count(Select(alice, FOLLOWS, ()).toThrift) must eventually(be_==(10))
      edges.count(Select(bob, FOLLOWS, ()).toThrift) must eventually(be_==(6))
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
      edges.select(program.toJavaList, new Page(1, Cursor.Start.position)) mustEqual result

      result = new Results(List[Long](bob).pack, Cursor.End.position, -bob)
      edges.select(program.toJavaList, new Page(1, darcy)) mustEqual result

      result = new Results(List[Long](darcy, bob).pack, Cursor.End.position, Cursor.End.position)
      edges.select(program.toJavaList, new Page(2, Cursor.Start.position)) mustEqual result
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
      edges.select(program.toJavaList, new Page(10, Cursor.Start.position)) mustEqual result
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
      edges.select(program.toJavaList, new Page(10, Cursor.Start.position)) mustEqual new Results(List[Long](9,7,5,3,1).pack, Cursor.End.position, Cursor.End.position)
      edges.select(program.toJavaList, new Page(2, Cursor.Start.position)) mustEqual new Results(List[Long](9,7).pack, 7, Cursor.End.position)
      edges.select(program.toJavaList, new Page(2, 7)) mustEqual new Results(List[Long](5,3).pack, 3, -5)
      edges.select(program.toJavaList, new Page(2, 3)) mustEqual new Results(List[Long](1).pack, Cursor.End.position, -1)
    }
  }
}
