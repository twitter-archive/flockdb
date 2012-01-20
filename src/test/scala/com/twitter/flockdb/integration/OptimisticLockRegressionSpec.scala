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

import org.specs.mock.{ClassMocker, JMocker}
import org.specs.util.{Duration => SpecsDuration}
import org.specs.matcher.Matcher
import com.twitter.gizzard.scheduler.{JsonJob, PrioritizingJobScheduler}
import com.twitter.gizzard.shards._
import com.twitter.gizzard.nameserver.NameServer
import com.twitter.util.Time
import com.twitter.conversions.time._
import com.twitter.flockdb.operations._
import jobs.single._
import shards.{Shard, SqlShard}


class OptimisticLockRegressionSpec extends IntegrationSpecification() {
  val FOLLOWS = 1
  val alice = 1

  val MIN = 3
  val MAX = 100
  val errorLimit = 5

  "Inserting conflicting items" should {
    "recover via the optimistic lock" in {
      reset(config)

      val scheduler = flock.jobScheduler(Priority.High.id)
      val errors = scheduler.errorQueue

      // No thrift api for this, so this is the best I know how to do.
      scheduler.put(new Single(1, FOLLOWS, 5106, 123456, State.Normal, Time.now, flock.forwardingManager, OrderedUuidGenerator))

      execute(Select(1, FOLLOWS, ()).archive)

      playNormalJobs()

      var found = false
      while (errors.size > 0) {
        val job = errors.get.get.job
        if (job.errorMessage.indexOf("lost optimistic lock") > 0) {
          found = true
        }
        job()
      }
      playScheduledJobs()

      found mustEqual true

      flockService.get(1, FOLLOWS, 5106)().state must eventually(be_==(State.Archived))
    }


    "still work even if we spam a ton of operations" in {
      // println("gogo")
      reset(config)

      val scheduler = flock.jobScheduler(Priority.High.id)
      val errors = scheduler.errorQueue

      // println("spamming edges")
      for(i <- 1 to 500) {
        (i % 2) match {
          case 0 => execute(Select(1, FOLLOWS, i).add)
          case 1 => execute(Select(1, FOLLOWS, i).archive)
        }
      }

      // println("spamming removes")
      for(i <- 1 to 50) {
        execute(Select((), FOLLOWS, i * 10).remove)
      }

      // println("spamming bulks")
      for(i <- 1 to 10) {
        (i % 2) match {
          case 0 => execute(Select(1, FOLLOWS, ()).add)
          case 1 => execute(Select(1, FOLLOWS, ()).archive)
        }
      }

      // println("final state")
      execute(Select(1, FOLLOWS, ()).archive)

      // println("draining")
      playNormalJobs()

      while (errors.size > 0) {
        // println("looping through the error queue")
        val job = errors.get.get.job
        try {
          job()
        } catch {
          case e => {
            job.errorCount += 1
            job.errorMessage = e.toString
            if (job.errorCount > errorLimit) {
              throw new RuntimeException("too many bad jobs")
            } else {
              errors.put(job)
            }
          }
        }

        playNormalJobs()
      }

      Thread.sleep(1000)

      val selectArchived = SimpleSelect(
        SelectOperation(
          SelectOperationType.SimpleQuery,
          Some(QueryTerm(alice, FOLLOWS, true, None, List(State.Archived)))
        )
      )

      count(selectArchived) must eventually(be_==(450))
      count(Select(1, FOLLOWS, ())) mustEqual 0

      for(i <- 1 to 500) {
        (i % 10) match {
          case 0 => ()
          case _ => flockService.get(1, FOLLOWS, i)().state mustEqual State.Archived
        }
      }
    }
  }
}
