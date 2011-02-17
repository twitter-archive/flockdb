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
import com.twitter.ostrich.Stats
import com.twitter.util.Time
import com.twitter.util.TimeConversions._
import com.twitter.flockdb
import thrift._
import conversions.ExecuteOperations._
import conversions.SelectOperation._
import conversions.Metadata._

class EdgesSpec extends IntegrationSpecification {

  val FOLLOWS = 1
  val BORKEN = 900

  val alice = 1L
  val bob = 2L
  val carl = 3L
  val darcy = 4L

  "Edge Integration" should {
    "contains_metadata"  in {
      reset(config)
      flock.contains_metadata(alice, FOLLOWS) must eventually(be_==(false))
      flock.execute(Select(alice, FOLLOWS, bob).add.toThrift)
      flock.contains_metadata(alice, FOLLOWS) must eventually(be_==(true))
    }

    "get_metadata"  in {
      reset(config)
      Time.withCurrentTimeFrozen { time =>
        flock.contains_metadata(alice, FOLLOWS) must eventually(be_==(false))
        flock.execute(Select(alice, FOLLOWS, bob).add.toThrift)
        flock.contains_metadata(alice, FOLLOWS) must eventually(be_==(true))

        // updated_at should not be confused with created_at.  Flock rows are commonly inserted with updated_at t=0.
        // This is done to make their sort order low, and prevents a race condition in the case where in an empty db:
        //
        //      1. Mark alice archived.
        //      2. Wait 1 second
        //      3. Insert edge between alice and bob
        //      4. Play those two operations in the db out of order.
        //      5. Observe that alice is unfortunately still in the normal state.
        //
        flock.get_metadata(alice, FOLLOWS) must eventually (be_==(flockdb.Metadata(alice, State.Normal, 1, new Time(0)).toThrift))

      }
    }

    "add" in {
      "existing graph" in {
        reset(config)
        Time.withCurrentTimeFrozen { time =>
          flock.execute(Select(alice, FOLLOWS, bob).add.toThrift)
          val term = new QueryTerm(alice, FOLLOWS, true)
          term.setDestination_ids(List[Long](bob).pack)
          term.setState_ids(List[Int](State.Normal.id).toJavaList)
          val op = new SelectOperation(SelectOperationType.SimpleQuery)
          op.setTerm(term)
          val page = new Page(1, Cursor.Start.position)
          flock.select(List(op).toJavaList, page).ids.array.size must eventually(be_>(0))
          time.advance(1.second)
          flock.execute(Select(alice, FOLLOWS, bob).remove.toThrift)
          flock.select(List(op).toJavaList, page).ids.array.size must eventually(be_==(0))
          flock.count(Select(alice, FOLLOWS, Nil).toThrift) mustEqual 0
        }
      }

      "nonexistent graph" in {
        reset(config)
        flock.execute(Select(alice, BORKEN, bob).add.toThrift) must throwA[FlockException]
      }
    }

    "remove" in {
      reset(config)
      flock.execute(Select(bob, FOLLOWS, alice).remove.toThrift)
      (!flock.contains(bob, FOLLOWS, alice) &&
        flock.count(Select(alice, FOLLOWS, Nil).toThrift) == 0 &&
        flock.count(Select(Nil, FOLLOWS, alice).toThrift) == 0) must eventually(beTrue)
    }

    "archive" in {
      reset(config)
      Time.withCurrentTimeFrozen { time =>
        flock.execute(Select(alice, FOLLOWS, bob).add.toThrift)
        flock.execute(Select(alice, FOLLOWS, carl).add.toThrift)
        flock.execute(Select(alice, FOLLOWS, darcy).add.toThrift)
        flock.execute(Select(darcy, FOLLOWS, alice).add.toThrift)
        (flock.count(Select(alice, FOLLOWS, ()).toThrift) == 3 &&
          flock.count(Select((), FOLLOWS, alice).toThrift) == 1) must eventually(beTrue)
        for (destinationId <- List(bob, carl, darcy)) {
          flock.count(Select((), FOLLOWS, destinationId).toThrift) must eventually(be_==(1))
        }
        flock.count(Select(darcy, FOLLOWS, ()).toThrift) must eventually(be_==(1))

        time.advance(1.second)
        flock.execute((Select(alice, FOLLOWS, ()).archive + Select((), FOLLOWS, alice).archive).toThrift)
        (flock.count(Select(alice, FOLLOWS, ()).toThrift) == 0 &&
          flock.count(Select((), FOLLOWS, alice).toThrift) == 0) must eventually(beTrue)
        for (destinationId <- List(bob, carl, darcy)) {
          flock.count(Select((), FOLLOWS, destinationId).toThrift) must eventually(be_==(0))
        }
        flock.count(Select(darcy, FOLLOWS, ()).toThrift) must eventually(be_==(0))

        time.advance(1.seconds)
        flock.execute((Select(alice, FOLLOWS, ()).add + Select((), FOLLOWS, alice).add).toThrift)
        (flock.count(Select(alice, FOLLOWS, ()).toThrift) == 3 &&
          flock.count(Select((), FOLLOWS, alice).toThrift) == 1) must eventually(beTrue)
        for (destinationId <- List(bob, carl, darcy)) {
          flock.count(Select((), FOLLOWS, destinationId).toThrift) must eventually(be_==(1))
        }
        flock.count(Select(darcy, FOLLOWS, ()).toThrift) must eventually(be_==(1))
      }
    }

    "archive & unarchive concurrently" in {
      reset(config)
      flock.execute(Select(alice, FOLLOWS, bob).add.toThrift)
      flock.execute(Select(alice, FOLLOWS, carl).add.toThrift)
      flock.execute(Select(alice, FOLLOWS, darcy).add.toThrift)
      flock.count(Select(alice, FOLLOWS, ()).toThrift) must eventually(be_==(3))
      for (destinationId <- List(bob, carl, darcy)) {
        flock.count(Select((), FOLLOWS, destinationId).toThrift) must eventually(be_==(1))
      }

      val archiveTime = 2.seconds.fromNow
      flock.execute((Select(alice, FOLLOWS, ()).addAt(archiveTime) +
                     Select((), FOLLOWS, alice).addAt(archiveTime)).toThrift)
      archiveTime.inSeconds must eventually(be_==(flock.get(alice, FOLLOWS, bob).updated_at))
      flock.execute((Select(alice, FOLLOWS, ()).archiveAt(archiveTime - 1.second) +
                     Select((), FOLLOWS, alice).archiveAt(archiveTime - 1.second)).toThrift)

      flock.count(Select(alice, FOLLOWS, ()).toThrift) must eventually(be_==(3))
      for (destinationId <- List(bob, carl, darcy)) {
        flock.count(Select((), FOLLOWS, destinationId).toThrift) must eventually(be_==(1))
      }
    }

    "toggle polarity" in {
      reset(config)
      Time.withCurrentTimeFrozen { time =>
        flock.execute(Select(alice, FOLLOWS, bob).add.toThrift)
        flock.execute(Select(alice, FOLLOWS, carl).add.toThrift)
        flock.execute(Select(alice, FOLLOWS, darcy).add.toThrift)
        flock.count(Select(alice, FOLLOWS, ()).toThrift) must eventually(be_==(3))
        time.advance(1.second)
        flock.execute(Select(alice, FOLLOWS, ()).negate.toThrift)
        flock.count(Select(alice, FOLLOWS, ()).toThrift) must eventually(be_==(0))
        flock.count(Select(alice, FOLLOWS, ()).negative.toThrift) must eventually(be_==(3))
        time.advance(1.second)
        flock.execute(Select(alice, FOLLOWS, ()).add.toThrift)
        flock.count(Select(alice, FOLLOWS, ()).toThrift) must eventually(be_==(3))
        flock.count(Select(alice, FOLLOWS, ()).negative.toThrift) must eventually(be_==(0))
      }
    }

    "counts" in {
      reset(config)
      flock.execute(Select(alice, FOLLOWS, bob).add.toThrift)
      flock.execute(Select(alice, FOLLOWS, carl).add.toThrift)
      flock.execute(Select(alice, FOLLOWS, darcy).add.toThrift)

      flock.count2(List(Select((), FOLLOWS, bob).toThrift,
                        Select((), FOLLOWS, carl).toThrift,
                        Select((), FOLLOWS, darcy).toThrift).toJavaList).toIntArray.toList must eventually(be_==(List(1, 1, 1)))
    }

    "select_edges" in {
      "simple query" in {
        reset(config)
        Time.withCurrentTimeFrozen { time =>
          flock.execute(Select(alice, FOLLOWS, bob).add.toThrift)
          time.advance(1.second)
          flock.execute(Select(alice, FOLLOWS, carl).add.toThrift)
          flock.count(Select(alice, FOLLOWS, ()).toThrift) must eventually(be_==(2))

          val term = new QueryTerm(alice, FOLLOWS, true)
          term.setState_ids(List[Int](State.Normal.id).toJavaList)
          val query = new EdgeQuery(term, new Page(10, Cursor.Start.position))
          val resultsList = flock.select_edges(List[EdgeQuery](query).toJavaList).toList
          resultsList.size mustEqual 1
          val results = resultsList(0)
          results.next_cursor mustEqual Cursor.End.position
          results.prev_cursor mustEqual Cursor.End.position
          results.edges.toList.map { _.destination_id }.toList mustEqual List[Long](carl, bob)
        }
      }

      "intersection" in {
        reset(config)
        flock.execute(Select(alice, FOLLOWS, bob).add.toThrift)
        flock.execute(Select(alice, FOLLOWS, carl).add.toThrift)
        flock.count(Select(alice, FOLLOWS, ()).toThrift) must eventually(be_==(2))

        val term = new QueryTerm(alice, FOLLOWS, true)
        term.setDestination_ids(List[Long](carl, darcy).pack)
        term.setState_ids(List[Int](State.Normal.id).toJavaList)
        val query = new EdgeQuery(term, new Page(10, Cursor.Start.position))
        val resultsList = flock.select_edges(List[EdgeQuery](query).toJavaList).toList
        resultsList.size mustEqual 1
        val results = resultsList(0)
        results.next_cursor mustEqual Cursor.End.position
        results.prev_cursor mustEqual Cursor.End.position
        results.edges.toList.map { _.destination_id }.toList mustEqual List[Long](carl)
      }
    }
  }
}
