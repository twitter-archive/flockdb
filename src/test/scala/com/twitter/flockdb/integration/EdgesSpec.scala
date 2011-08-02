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
import com.twitter.gizzard.thrift.conversions.Sequences._
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
      flockService.contains_metadata(alice, FOLLOWS) must eventually(be_==(false))
      flockService.execute(Select(alice, FOLLOWS, bob).add.toThrift)
      flockService.contains_metadata(alice, FOLLOWS) must eventually(be_==(true))
    }

    "get_metadata"  in {
      reset(config)
      flockService.contains_metadata(alice, FOLLOWS) must eventually(be_==(false))
      flockService.execute(Select(alice, FOLLOWS, bob).add.toThrift)
      flockService.contains_metadata(alice, FOLLOWS) must eventually(be_==(true))

      // updated_at should not be confused with created_at.  Flock rows are commonly inserted with updated_at t=0.
      // This is done to make their sort order low, and prevents a race condition in the case where in an empty db:
      //
      //      1. Mark alice archived.
      //      2. Wait 1 second
      //      3. Insert edge between alice and bob
      //      4. Play those two operations in the db out of order.
      //      5. Observe that alice is unfortunately still in the normal state.
      //
      flockService.get_metadata(alice, FOLLOWS) must eventually (be_==(new flockdb.Metadata(alice, State.Normal, 1, Time.epoch).toThrift))
    }

    "add" in {
      "existing graph" in {
        reset(config)
        flockService.execute(Select(alice, FOLLOWS, bob).add.toThrift)
        val term = new QueryTerm(alice, FOLLOWS, true)
        term.setDestination_ids(List[Long](bob).pack)
        term.setState_ids(List[Int](State.Normal.id))
        val op = new SelectOperation(SelectOperationType.SimpleQuery)
        op.setTerm(term)
        val page = new Page(1, Cursor.Start.position)
        flockService.select(List(op), page).ids.array.size must eventually(be_>(0))
        Thread.sleep(1000)
        flockService.execute(Select(alice, FOLLOWS, bob).remove.toThrift)
        flockService.select(List(op), page).ids.array.size must eventually(be_==(0))
        flockService.count(Select(alice, FOLLOWS, Nil).toThrift) mustEqual 0
      }

      "nonexistent graph" in {
        reset(config)
        flockService.execute(Select(alice, BORKEN, bob).add.toThrift) must throwA[FlockException]
      }
    }

    "remove" in {
      reset(config)
      flockService.execute(Select(bob, FOLLOWS, alice).remove.toThrift)
      flockService.contains(bob, FOLLOWS, alice) must eventually(beFalse)
      flockService.count(Select(alice, FOLLOWS, Nil).toThrift) must eventually(be_==(0))
      flockService.count(Select(Nil, FOLLOWS, alice).toThrift) must eventually(be_==(0))
    }

    "archive" in {
      reset(config)
      flockService.execute(Select(alice, FOLLOWS, bob).add.toThrift)
      flockService.execute(Select(alice, FOLLOWS, carl).add.toThrift)
      flockService.execute(Select(alice, FOLLOWS, darcy).add.toThrift)
      flockService.execute(Select(darcy, FOLLOWS, alice).add.toThrift)
      flockService.count(Select(alice, FOLLOWS, ()).toThrift) must eventually(be_==(3))
      flockService.count(Select((), FOLLOWS, alice).toThrift) must eventually(be_==(1))
      for (destinationId <- List(bob, carl, darcy)) {
        flockService.count(Select((), FOLLOWS, destinationId).toThrift) must eventually(be_==(1))
      }
      flockService.count(Select(darcy, FOLLOWS, ()).toThrift) must eventually(be_==(1))

      Thread.sleep(1000)
      flockService.execute((Select(alice, FOLLOWS, ()).archive + Select((), FOLLOWS, alice).archive).toThrift)
      flockService.count(Select(alice, FOLLOWS, ()).toThrift) must eventually(be_==(0))
      flockService.count(Select((), FOLLOWS, alice).toThrift) must eventually(be_==(0))
      for (destinationId <- List(bob, carl, darcy)) {
        flockService.count(Select((), FOLLOWS, destinationId).toThrift) must eventually(be_==(0))
      }
      flockService.count(Select(darcy, FOLLOWS, ()).toThrift) must eventually(be_==(0))

      Thread.sleep(1000)
      flockService.execute((Select(alice, FOLLOWS, ()).add + Select((), FOLLOWS, alice).add).toThrift)
      flockService.count(Select(alice, FOLLOWS, ()).toThrift) must eventually(be_==(3))
      flockService.count(Select((), FOLLOWS, alice).toThrift) must eventually(be_==(1))
      for (destinationId <- List(bob, carl, darcy)) {
        flockService.count(Select((), FOLLOWS, destinationId).toThrift) must eventually(be_==(1))
      }
      flockService.count(Select(darcy, FOLLOWS, ()).toThrift) must eventually(be_==(1))
    }

    "archive & unarchive concurrently" in {
      reset(config)
      flockService.execute(Select(alice, FOLLOWS, bob).add.toThrift)
      flockService.execute(Select(alice, FOLLOWS, carl).add.toThrift)
      flockService.execute(Select(alice, FOLLOWS, darcy).add.toThrift)
      flockService.count(Select(alice, FOLLOWS, ()).toThrift) must eventually(be_==(3))
      for (destinationId <- List(bob, carl, darcy)) {
        flockService.count(Select((), FOLLOWS, destinationId).toThrift) must eventually(be_==(1))
      }

      val archiveTime = 2.seconds.fromNow
      flockService.execute((Select(alice, FOLLOWS, ()).addAt(archiveTime) +
                     Select((), FOLLOWS, alice).addAt(archiveTime)).toThrift)
      archiveTime.inSeconds must eventually(be_==(flockService.get(alice, FOLLOWS, bob).updated_at))
      flockService.execute((Select(alice, FOLLOWS, ()).archiveAt(archiveTime - 1.second) +
                     Select((), FOLLOWS, alice).archiveAt(archiveTime - 1.second)).toThrift)

      flockService.count(Select(alice, FOLLOWS, ()).toThrift) must eventually(be_==(3))
      for (destinationId <- List(bob, carl, darcy)) {
        flockService.count(Select((), FOLLOWS, destinationId).toThrift) must eventually(be_==(1))
      }
    }

    "toggle polarity" in {
      reset(config)
      flockService.execute(Select(alice, FOLLOWS, bob).add.toThrift)
      flockService.execute(Select(alice, FOLLOWS, carl).add.toThrift)
      flockService.execute(Select(alice, FOLLOWS, darcy).add.toThrift)
      flockService.count(Select(alice, FOLLOWS, ()).toThrift) must eventually(be_==(3))
      Thread.sleep(1000)
      flockService.execute(Select(alice, FOLLOWS, ()).negate.toThrift)
      flockService.count(Select(alice, FOLLOWS, ()).toThrift) must eventually(be_==(0))
      flockService.count(Select(alice, FOLLOWS, ()).negative.toThrift) must eventually(be_==(3))
      Thread.sleep(1000)
      flockService.execute(Select(alice, FOLLOWS, ()).add.toThrift)
      flockService.count(Select(alice, FOLLOWS, ()).toThrift) must eventually(be_==(3))
      flockService.count(Select(alice, FOLLOWS, ()).negative.toThrift) must eventually(be_==(0))
    }

    "counts" in {
      reset(config)
      flockService.execute(Select(alice, FOLLOWS, bob).add.toThrift)
      flockService.execute(Select(alice, FOLLOWS, carl).add.toThrift)
      flockService.execute(Select(alice, FOLLOWS, darcy).add.toThrift)

      flockService.count2(List(Select((), FOLLOWS, bob).toThrift,
                        Select((), FOLLOWS, carl).toThrift,
                        Select((), FOLLOWS, darcy).toThrift)).toIntArray.toList must eventually(be_==(List(1, 1, 1)))
    }

    "select_edges" in {
      "simple query" in {
        reset(config)
        flockService.execute(Select(alice, FOLLOWS, bob).add.toThrift)
        Thread.sleep(1000)
        flockService.execute(Select(alice, FOLLOWS, carl).add.toThrift)
        flockService.count(Select(alice, FOLLOWS, ()).toThrift) must eventually(be_==(2))

        val term = new QueryTerm(alice, FOLLOWS, true)
        term.setState_ids(List[Int](State.Normal.id))
        val query = new EdgeQuery(term, new Page(10, Cursor.Start.position))
        val resultsList = flockService.select_edges(List[EdgeQuery](query)).toList
        resultsList.size mustEqual 1
        val results = resultsList(0)
        results.next_cursor mustEqual Cursor.End.position
        results.prev_cursor mustEqual Cursor.End.position
        results.edges.toList.map { _.destination_id }.toList mustEqual List[Long](carl, bob)
      }

      "intersection" in {
        reset(config)
        flockService.execute(Select(alice, FOLLOWS, bob).add.toThrift)
        flockService.execute(Select(alice, FOLLOWS, carl).add.toThrift)
        flockService.count(Select(alice, FOLLOWS, ()).toThrift) must eventually(be_==(2))

        val term = new QueryTerm(alice, FOLLOWS, true)
        term.setDestination_ids(List[Long](carl, darcy).pack)
        term.setState_ids(List[Int](State.Normal.id))
        val query = new EdgeQuery(term, new Page(10, Cursor.Start.position))
        val resultsList = flockService.select_edges(List[EdgeQuery](query)).toList
        resultsList.size mustEqual 1
        val results = resultsList(0)
        results.next_cursor mustEqual Cursor.End.position
        results.prev_cursor mustEqual Cursor.End.position
        results.edges.toList.map { _.destination_id }.toList mustEqual List[Long](carl)
      }
    }
  }
}
