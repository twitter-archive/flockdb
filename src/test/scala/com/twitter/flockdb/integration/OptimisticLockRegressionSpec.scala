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

import scala.collection._
import scala.collection.mutable.ArrayBuffer
import org.specs.mock.{ClassMocker, JMocker}
import org.specs.util.{Duration => SpecsDuration}
import org.specs.matcher.Matcher
import com.twitter.gizzard.scheduler.{JsonJob, PrioritizingJobScheduler}
import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.gizzard.shards._
import com.twitter.gizzard.nameserver.NameServer
import com.twitter.util.Time
import com.twitter.util.TimeConversions._
import com.twitter.flockdb
import com.twitter.flockdb.{SelectQuery, Metadata}
import jobs.single._
import shards.{Shard, SqlShard}
import thrift._


class OptimisticLockRegressionSpec extends IntegrationSpecification() {
  val FOLLOWS = 1
  val alice = 1

  val MIN = 3
  val MAX = 100
  val errorLimit = 5

  //override def eventually[T](nested: Matcher[T]): Matcher[T] = eventually(100, new SpecsDuration(2000))(nested)

  "Inserting conflicting items" should {
    "recover via the optimistic lock" in {
      reset(config)

      val scheduler = flock.jobScheduler(flockdb.Priority.High.id)
      val errors = scheduler.errorQueue

      // No thrift api for this, so this is the best I know how to do.
      scheduler.put(new Single(1, FOLLOWS, 5106, 123456, State.Normal, Time.now, flock.forwardingManager, OrderedUuidGenerator, NoOpFilter))

      flockService.execute(Select(1, FOLLOWS, ()).archive.toThrift)

      jobSchedulerMustDrain

      //flockService.contains(1, FOLLOWS, 5106) must eventually(be_==(true))
      //flockService.get(1, FOLLOWS, 5106).state_id must eventually(be_==(State.Normal.id))

      var found = false
      while (errors.size > 0) {
        val job = errors.get.get.job
        if (job.errorMessage.indexOf("lost optimistic lock") > 0) {
          found = true
        }
        job()
      }
      jobSchedulerMustDrain

      found mustEqual true

      flockService.get(1, FOLLOWS, 5106).state_id must eventually(be_==(State.Archived.id))
    }


    "still work even if we spam a ton of operations" in {
      // println("gogo")
      reset(config)

      val scheduler = flock.jobScheduler(flockdb.Priority.High.id)
      val errors = scheduler.errorQueue

      // println("spamming edges")
      for(i <- 1 to 500) {
        (i % 2) match {
          case 0 => flockService.execute(Select(1, FOLLOWS, i).add.toThrift)
          case 1 => flockService.execute(Select(1, FOLLOWS, i).archive.toThrift)
        }
      }

      // println("spamming removes")
      for(i <- 1 to 50) {
        flockService.execute(Select((), FOLLOWS, i * 10).remove.toThrift)
      }

      // println("spamming bulks")
      for(i <- 1 to 10) {
        (i % 2) match {
          case 0 => flockService.execute(Select(1, FOLLOWS, ()).add.toThrift)
          case 1 => flockService.execute(Select(1, FOLLOWS, ()).archive.toThrift)
        }
      }

      // println("final state")
      flockService.execute(Select(1, FOLLOWS, ()).archive.toThrift)

      // println("draining")

      while(scheduler.size > 0) {
        // print(scheduler.size)
        Thread.sleep(10)
      }
      jobSchedulerMustDrain

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

        jobSchedulerMustDrain
      }

      Thread.sleep(1000)

      val selectArchived =     new SimpleSelect(new operations.SelectOperation(operations.SelectOperationType.SimpleQuery, Some(new flockdb.QueryTerm(alice, FOLLOWS, true, None, List(State.Archived)))))

      flockService.count(selectArchived.toThrift) must eventually(be_==(450))
      flockService.count(Select(1, FOLLOWS, ()).toThrift) mustEqual 0

      for(i <- 1 to 500) {
        (i % 10) match {
          case 0 => ()
          case _ => flockService.get(1, FOLLOWS, i).state_id mustEqual State.Archived.id
        }
      }
    }
  }
}
