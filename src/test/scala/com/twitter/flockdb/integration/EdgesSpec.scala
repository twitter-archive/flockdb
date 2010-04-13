package com.twitter.flockdb.integration

import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.results.Cursor
import com.twitter.ostrich.Stats
import com.twitter.xrayspecs.{Eventually, Time}
import com.twitter.xrayspecs.TimeConversions._
import net.lag.configgy.Configgy
import net.lag.smile.kestrel.{KestrelClient, MemoryStore}
import org.specs.Specification
import thrift._
import com.twitter.service.flock.State
import conversions.ExecuteOperations._
import conversions.SelectOperation._


object EdgesSpec extends Specification with EdgesReset with Eventually {
  import StaticEdges._

  val FOLLOWS = 1

  val alice = 1L
  val bob = 2L
  val carl = 3L
  val darcy = 4L

  "Edge Integration" should {
    doBefore {
      edges
      reset(Configgy.config.configMap("edges"))
      reset(edges)
      Time.freeze()
    }

    "add" in {
      edges.execute(Select(alice, FOLLOWS, bob).add.toThrift)
      val term = new QueryTerm(alice, FOLLOWS, true)
      term.setDestination_ids(List[Long](bob).pack)
      term.setState_ids(List[Int](State.Normal.id).toJavaList)
      val op = new SelectOperation(SelectOperationType.SimpleQuery)
      op.setTerm(term)
      val page = new Page(1, Cursor.Start.position)
      edges.select(List(op).toJavaList, page).ids.size must eventually(be_>(0))
      Time.advance(1.second)
      edges.execute(Select(alice, FOLLOWS, bob).remove.toThrift)
      edges.select(List(op).toJavaList, page).ids.size must eventually(be_==(0))
      edges.count(Select(alice, FOLLOWS, Nil).toThrift) mustEqual 0
    }

    "remove" in {
      edges.execute(Select(bob, FOLLOWS, alice).remove.toThrift)
      (!edges.contains(bob, FOLLOWS, alice) &&
        edges.count(Select(alice, FOLLOWS, Nil).toThrift) == 0 &&
        edges.count(Select(Nil, FOLLOWS, alice).toThrift) == 0) must eventually(beTrue)
    }

    "archive" in {
      edges.execute(Select(alice, FOLLOWS, bob).add.toThrift)
      edges.execute(Select(alice, FOLLOWS, carl).add.toThrift)
      edges.execute(Select(alice, FOLLOWS, darcy).add.toThrift)
      edges.execute(Select(darcy, FOLLOWS, alice).add.toThrift)
      (edges.count(Select(alice, FOLLOWS, ()).toThrift) == 3 &&
        edges.count(Select((), FOLLOWS, alice).toThrift) == 1) must eventually(beTrue)
      for (destinationId <- List(bob, carl, darcy)) {
        edges.count(Select((), FOLLOWS, destinationId).toThrift) must eventually(be_==(1))
      }
      edges.count(Select(darcy, FOLLOWS, ()).toThrift) must eventually(be_==(1))

      Time.advance(1.second)
      edges.execute((Select(alice, FOLLOWS, ()).archive + Select((), FOLLOWS, alice).archive).toThrift)
      (edges.count(Select(alice, FOLLOWS, ()).toThrift) == 0 &&
        edges.count(Select((), FOLLOWS, alice).toThrift) == 0) must eventually(beTrue)
      for (destinationId <- List(bob, carl, darcy)) {
        edges.count(Select((), FOLLOWS, destinationId).toThrift) must eventually(be_==(0))
      }
      edges.count(Select(darcy, FOLLOWS, ()).toThrift) must eventually(be_==(0))

      Time.advance(1.seconds)
      edges.execute((Select(alice, FOLLOWS, ()).add + Select((), FOLLOWS, alice).add).toThrift)
      (edges.count(Select(alice, FOLLOWS, ()).toThrift) == 3 &&
        edges.count(Select((), FOLLOWS, alice).toThrift) == 1) must eventually(beTrue)
      for (destinationId <- List(bob, carl, darcy)) {
        edges.count(Select((), FOLLOWS, destinationId).toThrift) must eventually(be_==(1))
      }
      edges.count(Select(darcy, FOLLOWS, ()).toThrift) must eventually(be_==(1))
    }

    "archive & unarchive concurrently" in {
      edges.execute(Select(alice, FOLLOWS, bob).add.toThrift)
      edges.execute(Select(alice, FOLLOWS, carl).add.toThrift)
      edges.execute(Select(alice, FOLLOWS, darcy).add.toThrift)
      edges.count(Select(alice, FOLLOWS, ()).toThrift) must eventually(be_==(3))
      for (destinationId <- List(bob, carl, darcy)) {
        edges.count(Select((), FOLLOWS, destinationId).toThrift) must eventually(be_==(1))
      }

      val archiveTime = 2.seconds.fromNow
      edges.execute((Select(alice, FOLLOWS, ()).addAt(archiveTime) +
                     Select((), FOLLOWS, alice).addAt(archiveTime)).toThrift)
      archiveTime.inSeconds must eventually(be_==(edges.get(alice, FOLLOWS, bob).updated_at))
      edges.execute((Select(alice, FOLLOWS, ()).archiveAt(archiveTime - 1.second) +
                     Select((), FOLLOWS, alice).archiveAt(archiveTime - 1.second)).toThrift)

      edges.count(Select(alice, FOLLOWS, ()).toThrift) must eventually(be_==(3))
      for (destinationId <- List(bob, carl, darcy)) {
        edges.count(Select((), FOLLOWS, destinationId).toThrift) must eventually(be_==(1))
      }
    }

    "toggle polarity" in {
      edges.execute(Select(alice, FOLLOWS, bob).add.toThrift)
      edges.execute(Select(alice, FOLLOWS, carl).add.toThrift)
      edges.execute(Select(alice, FOLLOWS, darcy).add.toThrift)
      edges.count(Select(alice, FOLLOWS, ()).toThrift) must eventually(be_==(3))
      Time.advance(1.second)
      edges.execute(Select(alice, FOLLOWS, ()).negate.toThrift)
      edges.count(Select(alice, FOLLOWS, ()).toThrift) must eventually(be_==(0))
      edges.count(Select(alice, FOLLOWS, ()).negative.toThrift) must eventually(be_==(3))
      Time.advance(1.second)
      edges.execute(Select(alice, FOLLOWS, ()).add.toThrift)
      edges.count(Select(alice, FOLLOWS, ()).toThrift) must eventually(be_==(3))
      edges.count(Select(alice, FOLLOWS, ()).negative.toThrift) must eventually(be_==(0))
    }

    "counts" in {
      edges.execute(Select(alice, FOLLOWS, bob).add.toThrift)
      edges.execute(Select(alice, FOLLOWS, carl).add.toThrift)
      edges.execute(Select(alice, FOLLOWS, darcy).add.toThrift)

      edges.counts_of_sources_for(List[Long](bob, carl, darcy).pack, FOLLOWS).toList must eventually(be_==(List[Int](1, 1, 1).pack.toList))
    }

    "select_edges" in {
      "simple query" in {
        edges.execute(Select(alice, FOLLOWS, bob).add.toThrift)
        Time.advance(1.second)
        edges.execute(Select(alice, FOLLOWS, carl).add.toThrift)
        edges.count(Select(alice, FOLLOWS, ()).toThrift) must eventually(be_==(2))

        val term = new QueryTerm(alice, FOLLOWS, true)
        term.setState_ids(List[Int](State.Normal.id).toJavaList)
        val query = new EdgeQuery(term, new Page(10, Cursor.Start.position))
        val resultsList = edges.select_edges(List[EdgeQuery](query).toJavaList).toList
        resultsList.size mustEqual 1
        val results = resultsList(0)
        results.next_cursor mustEqual Cursor.End.position
        results.prev_cursor mustEqual Cursor.End.position
        results.edges.toList.map { _.destination_id }.toList mustEqual List[Long](carl, bob)
      }

      "intersection" in {
        edges.execute(Select(alice, FOLLOWS, bob).add.toThrift)
        edges.execute(Select(alice, FOLLOWS, carl).add.toThrift)
        edges.count(Select(alice, FOLLOWS, ()).toThrift) must eventually(be_==(2))

        val term = new QueryTerm(alice, FOLLOWS, true)
        term.setDestination_ids(List[Long](carl, darcy).pack)
        term.setState_ids(List[Int](State.Normal.id).toJavaList)
        val query = new EdgeQuery(term, new Page(10, Cursor.Start.position))
        val resultsList = edges.select_edges(List[EdgeQuery](query).toJavaList).toList
        resultsList.size mustEqual 1
        val results = resultsList(0)
        results.next_cursor mustEqual Cursor.End.position
        results.prev_cursor mustEqual Cursor.End.position
        results.edges.toList.map { _.destination_id }.toList mustEqual List[Long](carl)
      }
    }
  }
}
