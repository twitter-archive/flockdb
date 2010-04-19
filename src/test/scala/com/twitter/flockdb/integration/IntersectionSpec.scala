package com.twitter.flockdb.integration

import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.querulous.evaluator.QueryEvaluatorFactory
import com.twitter.results.Cursor
import com.twitter.xrayspecs.Eventually
import thrift.{Page, QueryTerm, Results, SelectOperation, SelectOperationType}


object IntersectionSpec extends ConfiguredSpecification with Eventually with EdgesDatabase {
  val poolConfig = config.configMap("db.connection_pool")

  import StaticEdges._

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
    edges.select(List[SelectOperation](
      op1,
      op2,
      new SelectOperation(SelectOperationType.Intersection)).toJavaList, page)
  }

  def intersectAlot = {
    "intersection_for" in {
      "pagination" in {
        edges.execute(Select(alice, FOLLOWS, bob).add.toThrift)
        edges.execute(Select(alice, FOLLOWS, carl).add.toThrift)
        edges.execute(Select(alice, FOLLOWS, darcy).add.toThrift)
        edges.execute(Select(carl, FOLLOWS, bob).add.toThrift)
        edges.execute(Select(carl, FOLLOWS, darcy).add.toThrift)
        edges.contains(carl, FOLLOWS, darcy) must eventually(beTrue)

        var result = new Results(List[Long](darcy).pack, darcy, Cursor.End.position)
        intersection_of(alice, carl, new Page(1, Cursor.Start.position)) mustEqual result

        result = new Results(List[Long](bob).pack, Cursor.End.position, -bob)
        intersection_of(alice, carl, new Page(1, darcy)) mustEqual result

        result = new Results(List[Long](darcy, bob).pack, Cursor.End.position, Cursor.End.position)
        intersection_of(alice, carl, new Page(2, Cursor.Start.position)) mustEqual result
      }

      "one list is empty" in {
        for (i <- 1 until 11) edges.execute(Select(alice, FOLLOWS, i).add.toThrift)
        edges.count(Select(alice, FOLLOWS, ()).toThrift) must eventually(be_==(10))

        var result = new Results(List[Long]().pack, Cursor.End.position, Cursor.End.position)
        intersection_of(alice, carl, new Page(10, Cursor.Start.position)) mustEqual result
      }
    }
  }

  "Intersection" should {
    doBefore {
      reset(edges)
    }

    "with a large intersection" >>  {
      doBefore { config("edges.intersection_page_size_max") = 1 }

      intersectAlot
    }

    "with a small intersection" >> {
      doBefore { config("edges.intersection_page_size_max") = Integer.MAX_VALUE - 1 }

      intersectAlot
    }
  }
}
