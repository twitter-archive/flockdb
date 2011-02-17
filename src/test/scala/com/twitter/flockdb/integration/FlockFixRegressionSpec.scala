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
import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.gizzard.shards.ShardInfo
import com.twitter.util.Time
import com.twitter.util.TimeConversions._
import com.twitter.flockdb
import com.twitter.flockdb.{SelectQuery, Metadata}
import org.specs.mock.{ClassMocker, JMocker}
import jobs.multi.{Archive, RemoveAll, Unarchive}
import jobs.single.{Add, Remove}
import shards.{Shard, SqlShard}
import thrift._

class FlockFixRegressionSpec extends IntegrationSpecification {
  val alice = 1L
  val FOLLOWS = 1
  val pageSize = 100

  def alicesFollowings() = {
    val term = new QueryTerm(alice, FOLLOWS, true)
    term.setState_ids(List[Int](State.Normal.id).toJavaList)
    val query = new EdgeQuery(term, new Page(pageSize, Cursor.Start.position))
    val resultsList = flock.select_edges(List[EdgeQuery](query).toJavaList).toList
    resultsList.size mustEqual 1
    resultsList(0).edges
  }

  "select results" should {
    "be in order and still in order after unarchive" in {
      reset(config)  // I don't know why this isn't working in doBefore

      for(i <- 0 until 10) {
        if (i % 2 == 0) {
          flock.execute(Select(alice, FOLLOWS, i).add.toThrift)
        } else {
          flock.execute(Select(alice, FOLLOWS, i).archive.toThrift)
        }
        Thread.sleep(2) // prevent same-millisecond collision
      }

      jobScheduler.size must eventually(be(0)) // Make sure adds get applied.  I can't wait for Time.asOf()

      alicesFollowings().size must eventually(be_==(5))
      alicesFollowings().toList.map(_.destination_id) mustEqual List(8,6,4,2,0)

      Thread.sleep(1000)

      val job = Unarchive(alice, FOLLOWS, Direction.Forward, Time.now, flockdb.Priority.High, pageSize, flock.edges.forwardingManager, flock.edges.schedule)
      job()

      alicesFollowings().size must eventually(be(10))

      alicesFollowings().toList.map(_.destination_id) mustEqual List(9, 8, 7, 6, 5, 4, 3, 2, 1, 0)
    }
  }

}
