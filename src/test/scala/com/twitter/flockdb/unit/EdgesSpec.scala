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
package unit

import scala.collection.mutable
import scala.collection.JavaConversions._
import com.twitter.gizzard.Future
import com.twitter.gizzard.scheduler._
import com.twitter.gizzard.nameserver.NameServer
import com.twitter.gizzard.shards.ShardInfo
import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.util.Time
import com.twitter.util.TimeConversions._
import org.specs.mock.{ClassMocker, JMocker}
import jobs.multi.{RemoveAll, Archive, Unarchive}
import jobs.single.{Add, Remove}
import conversions.Edge._
import shards.Shard
import thrift.{FlockException, Page, Results}
import State._


object EdgesSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  "Edges" should {
    val FOLLOWS = 1

    val bob = 1L
    val mary = 2L

    val nestedJob = capturingParam[JsonNestedJob]
    val nameServer = mock[NameServer[Shard]]
    val uuidGenerator = mock[UuidGenerator]
    val forwardingManager = mock[ForwardingManager]
    val shard = mock[Shard]
    val scheduler = mock[PrioritizingJobScheduler]
    val future = mock[Future]
    val copyFactory = mock[CopyJobFactory[Shard]]
    val flock = new FlockDBThriftAdapter(new EdgesService(nameServer, forwardingManager, copyFactory, scheduler, future, config.intersectionQuery, config.aggregateJobsPageSize, IdentityUuidGenerator), null)

    "add" in {
      Time.withCurrentTimeFrozen { time =>
        val job = Add(bob, FOLLOWS, mary, Time.now.inMillis, Time.now, null, null)
        expect {
          one(forwardingManager).find(0, FOLLOWS, Direction.Forward)
          one(scheduler).put(will(beEqual(Priority.High.id)), nestedJob.capture)
        }
        flock.execute(Select(bob, FOLLOWS, mary).add.toThrift)
        jsonMatching(List(job), nestedJob.captured.jobs)
      }
    }

    "add_at" in {
      Time.withCurrentTimeFrozen { time =>
        val job = Add(bob, FOLLOWS, mary, Time.now.inMillis, Time.now, null, null)
        expect {
          one(forwardingManager).find(0, FOLLOWS, Direction.Forward)
          one(scheduler).put(will(beEqual(Priority.High.id)), nestedJob.capture)
        }
        flock.execute(Select(bob, FOLLOWS, mary).addAt(Time.now).toThrift)
        jsonMatching(List(job), nestedJob.captured.jobs)
      }
    }

    "remove" in {
      Time.withCurrentTimeFrozen { time =>
        val job = Remove(bob, FOLLOWS, mary, Time.now.inMillis, Time.now, null, null)
        expect {
          one(forwardingManager).find(0, FOLLOWS, Direction.Forward)
          one(scheduler).put(will(beEqual(Priority.High.id)), nestedJob.capture)
        }
        flock.execute(Select(bob, FOLLOWS, mary).remove.toThrift)
        jsonMatching(List(job), nestedJob.captured.jobs)
      }
    }

    "remove_at" in {
      Time.withCurrentTimeFrozen { time =>
        val job = Remove(bob, FOLLOWS, mary, Time.now.inMillis, Time.now, null, null)
        expect {
          one(forwardingManager).find(0, FOLLOWS, Direction.Forward)
          one(scheduler).put(will(beEqual(Priority.High.id)), nestedJob.capture)
        }
        flock.execute(Select(bob, FOLLOWS, mary).removeAt(Time.now).toThrift)
        jsonMatching(List(job), nestedJob.captured.jobs)
      }
    }

    "contains" in {
      Time.withCurrentTimeFrozen { time =>
        expect {
          one(forwardingManager).find(bob, FOLLOWS, Direction.Forward) willReturn shard
          one(shard).get(bob, mary) willReturn Some(new Edge(bob, mary, 0, Time.now, 0, State.Normal))
        }
        flock.contains(bob, FOLLOWS, mary) must beTrue
      }
    }
  }
}
