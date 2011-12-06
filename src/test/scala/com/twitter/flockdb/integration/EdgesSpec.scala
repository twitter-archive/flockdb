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
import com.twitter.flockdb.operations._
import com.twitter.flockdb.thrift.FlockException

class EdgesSpec extends IntegrationSpecification {

  val FOLLOWS = 1
  val BORKEN = 900

  val alice = 1L
  val bob = 2L
  val carl = 3L
  val darcy = 4L

  def counts(selects: Select*) = {
    flockService.count(selects map { _.toList })
  }

  "Edge Integration" should {
    "contains_metadata"  in {
      reset(config)
      flockService.containsMetadata(alice, FOLLOWS) must eventually(be_==(false))
      execute(Select(alice, FOLLOWS, bob).add)
      flockService.containsMetadata(alice, FOLLOWS) must eventually(be_==(true))
    }

    "get_metadata"  in {
      reset(config)
      flockService.containsMetadata(alice, FOLLOWS) must eventually(be_==(false))
      execute(Select(alice, FOLLOWS, bob).add)
      flockService.containsMetadata(alice, FOLLOWS) must eventually(be_==(true))

      // updated_at should not be confused with created_at.  Flock rows are commonly inserted with updated_at t=0.
      // This is done to make their sort order low, and prevents a race condition in the case where in an empty db:
      //
      //      1. Mark alice archived.
      //      2. Wait 1 second
      //      3. Insert edge between alice and bob
      //      4. Play those two operations in the db out of order.
      //      5. Observe that alice is unfortunately still in the normal state.
      //
      flockService.getMetadata(alice, FOLLOWS) must eventually (be_==(Metadata(alice, State.Normal, 1, Time.epoch)))
    }

    "add" in {
      "existing graph" in {
        reset(config)
        execute(Select(alice, FOLLOWS, bob).add)
        val term  = QueryTerm(alice, FOLLOWS, true, Some(List(bob)), List(State.Normal))
        val op    = SelectOperation(SelectOperationType.SimpleQuery, Some(term))
        val page  = Page(1, Cursor.Start)
        val query = SelectQuery(List(op), page)

        flockService.select(query).length must eventually(be_>(0))
        Thread.sleep(1000)

        execute(Select(alice, FOLLOWS, bob).remove)
        flockService.select(query).length must eventually(be_==(0))
        count(Select(alice, FOLLOWS, Nil)) mustEqual 0
      }

      "nonexistent graph" in {
        reset(config)
        execute(Select(alice, BORKEN, bob).add) must throwA[FlockException]
      }
    }

    "remove" in {
      reset(config)
      execute(Select(bob, FOLLOWS, alice).remove)
      flockService.contains(bob, FOLLOWS, alice) must eventually(beFalse)
      count(Select(alice, FOLLOWS, Nil)) must eventually(be_==(0))
      count(Select(Nil, FOLLOWS, alice)) must eventually(be_==(0))
    }

    "archive" in {
      reset(config)
      execute(Select(alice, FOLLOWS, bob).add)
      execute(Select(alice, FOLLOWS, carl).add)
      execute(Select(alice, FOLLOWS, darcy).add)
      execute(Select(darcy, FOLLOWS, alice).add)
      count(Select(alice, FOLLOWS, ())) must eventually(be_==(3))
      count(Select((), FOLLOWS, alice)) must eventually(be_==(1))
      for (destinationId <- List(bob, carl, darcy)) {
        count(Select((), FOLLOWS, destinationId)) must eventually(be_==(1))
      }
      count(Select(darcy, FOLLOWS, ())) must eventually(be_==(1))

      Thread.sleep(1000)
      execute(Select(alice, FOLLOWS, ()).archive + Select((), FOLLOWS, alice).archive)
      count(Select(alice, FOLLOWS, ())) must eventually(be_==(0))
      count(Select((), FOLLOWS, alice)) must eventually(be_==(0))
      for (destinationId <- List(bob, carl, darcy)) {
        count(Select((), FOLLOWS, destinationId)) must eventually(be_==(0))
      }
      count(Select(darcy, FOLLOWS, ())) must eventually(be_==(0))

      Thread.sleep(1000)
      execute(Select(alice, FOLLOWS, ()).add + Select((), FOLLOWS, alice).add)
      count(Select(alice, FOLLOWS, ())) must eventually(be_==(3))
      count(Select((), FOLLOWS, alice)) must eventually(be_==(1))
      for (destinationId <- List(bob, carl, darcy)) {
        count(Select((), FOLLOWS, destinationId)) must eventually(be_==(1))
      }
      count(Select(darcy, FOLLOWS, ())) must eventually(be_==(1))
    }

    "archive & unarchive concurrently" in {
      reset(config)
      execute(Select(alice, FOLLOWS, bob).add)
      execute(Select(alice, FOLLOWS, carl).add)
      execute(Select(alice, FOLLOWS, darcy).add)
      count(Select(alice, FOLLOWS, ())) must eventually(be_==(3))
      for (destinationId <- List(bob, carl, darcy)) {
        count(Select((), FOLLOWS, destinationId)) must eventually(be_==(1))
      }

      val archiveTime = 2.seconds.fromNow
      execute(Select(alice, FOLLOWS, ()).addAt(archiveTime) + Select((), FOLLOWS, alice).addAt(archiveTime), Some(archiveTime))
      archiveTime.inSeconds must eventually(be_==(flockService.get(alice, FOLLOWS, bob).updatedAt.inSeconds))
      execute(Select(alice, FOLLOWS, ()).archiveAt(archiveTime - 1.second) + Select((), FOLLOWS, alice).archiveAt(archiveTime - 1.second), Some(archiveTime - 1.second))

      count(Select(alice, FOLLOWS, ())) must eventually(be_==(3))
      for (destinationId <- List(bob, carl, darcy)) {
        count(Select((), FOLLOWS, destinationId)) must eventually(be_==(1))
      }
    }

    "toggle polarity" in {
      reset(config)
      execute(Select(alice, FOLLOWS, bob).add)
      execute(Select(alice, FOLLOWS, carl).add)
      execute(Select(alice, FOLLOWS, darcy).add)
      count(Select(alice, FOLLOWS, ())) must eventually(be_==(3))
      Thread.sleep(1000)
      execute(Select(alice, FOLLOWS, ()).negate)
      count(Select(alice, FOLLOWS, ())) must eventually(be_==(0))
      count(Select(alice, FOLLOWS, ()).negative) must eventually(be_==(3))
      Thread.sleep(1000)
      execute(Select(alice, FOLLOWS, ()).add)
      count(Select(alice, FOLLOWS, ())) must eventually(be_==(3))
      count(Select(alice, FOLLOWS, ()).negative) must eventually(be_==(0))
    }

    "counts" in {
      reset(config)
      execute(Select(alice, FOLLOWS, bob).add)
      execute(Select(alice, FOLLOWS, carl).add)
      execute(Select(alice, FOLLOWS, darcy).add)

      counts(Select((), FOLLOWS, bob),
             Select((), FOLLOWS, carl),
             Select((), FOLLOWS, darcy)) must eventually(be_==(List(1, 1, 1)))
    }

    "select_edges" in {
      "simple query" in {
        reset(config)
        execute(Select(alice, FOLLOWS, bob).add)
        Thread.sleep(1000)
        execute(Select(alice, FOLLOWS, carl).add)
        count(Select(alice, FOLLOWS, ())) must eventually(be_==(2))

        val term  = QueryTerm(alice, FOLLOWS, true, None, List(State.Normal))
        val query = EdgeQuery(term, new Page(10, Cursor.Start))
        val resultsList = flockService.selectEdges(List[EdgeQuery](query)).toList
        resultsList.size mustEqual 1
        val results = resultsList(0)
        results.nextCursor mustEqual Cursor.End
        results.prevCursor mustEqual Cursor.End
        results.map { _.destinationId }.toList mustEqual List(carl, bob)
      }

      "intersection" in {
        reset(config)
        execute(Select(alice, FOLLOWS, bob).add)
        execute(Select(alice, FOLLOWS, carl).add)
        count(Select(alice, FOLLOWS, ())) must eventually(be_==(2))

        val term = new QueryTerm(alice, FOLLOWS, true, Some(List(carl, darcy)), List(State.Normal))
        val query = new EdgeQuery(term, new Page(10, Cursor.Start))
        val resultsList = flockService.selectEdges(List[EdgeQuery](query)).toList
        resultsList.size mustEqual 1
        val results = resultsList(0)
        results.nextCursor mustEqual Cursor.End
        results.prevCursor mustEqual Cursor.End
        results.map { _.destinationId }.toList mustEqual List(carl)
      }
    }
  }
}
