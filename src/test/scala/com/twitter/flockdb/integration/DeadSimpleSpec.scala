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

package com.twitter.flockdb.integration

import org.specs.util.TimeConversions._

object DeadSimpleSpec extends IntegrationSpecification {
  "Concurrency" should {
    val FOLLOWS = 1

    val alice = 1L
    val bob = 2L
    val carl = 3L
    val darcy = 4L

    doBefore {
      reset(config)
    }

    "trivial changes" in {
      val scheduler = jobScheduler(flockdb.Priority.High.id)
      val errors = scheduler.errorQueue
      
      try {
        for (i <- 1 until 11) flock.execute(Select(alice, FOLLOWS, i).add.toThrift)
        for (i <- 1 until 7) flock.execute(Select(bob, FOLLOWS, i * 2).add.toThrift)
        flock.count(Select(alice, FOLLOWS, ()).toThrift) must eventually(20, 500.millis)(be_==(10))
        flock.count(Select(bob, FOLLOWS, ()).toThrift) must eventually(20, 500.millis)(be_==(6))
      } catch {
        case e: Throwable => {
          errors.size must eventually(be_>(0))
          while(errors.size > 0) {
            errors.get.foreach { ticket => println(ticket.job.toString) }
          }
          println(e.getMessage)
          fail
        }
      }
    }
  }
}
