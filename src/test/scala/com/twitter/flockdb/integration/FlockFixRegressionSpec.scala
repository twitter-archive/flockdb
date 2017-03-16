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

import com.twitter.gizzard.scheduler.{JsonJob, PrioritizingJobScheduler}
import com.twitter.gizzard.shards.ShardInfo
import com.twitter.util.Time
import com.twitter.conversions.time._
import org.specs.mock.{ClassMocker, JMocker}
import jobs.multi.Multi
import shards.{Shard, SqlShard}

class FlockFixRegressionSpec extends IntegrationSpecification {
  val alice = 1L
  val FOLLOWS = 1
  val pageSize = 100

  def alicesFollowings() = {
    val term = QueryTerm(alice, FOLLOWS, true, None, List(State.Normal))
    val query = EdgeQuery(term, Page(pageSize, Cursor.Start))
    val resultsList = flockService.selectEdges(List(query))()
    resultsList.size mustEqual 1
    resultsList(0).toList
  }

  "select results" should {
    "be in order and still in order after unarchive" in {
      reset(config)  // I don't know why this isn't working in doBefore

      for(i <- 0 until 10) {
        if (i % 2 == 0) {
          execute(Select(alice, FOLLOWS, i).add)
        } else {
          execute(Select(alice, FOLLOWS, i).archive)
        }
        Thread.sleep(1000) // prevent same-millisecond collision
      }

      flock.jobScheduler.size must eventually(be(0)) // Make sure adds get applied.  I can't wait for Time.asOf()

      alicesFollowings().size must eventually(be_==(5))
      alicesFollowings().toList.map(_.destinationId) mustEqual List(8,6,4,2,0)

      Thread.sleep(1000)

      val job = new Multi(alice, FOLLOWS, Direction.Forward, State.Normal, Time.now, Priority.High, pageSize, flock.forwardingManager, flock.jobScheduler, NoOpFilter)
      job()

      alicesFollowings().size must eventually(be(10))

      alicesFollowings().toList.map(_.destinationId) mustEqual List(9, 8, 7, 6, 5, 4, 3, 2, 1, 0)
    }
  }

}
