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

package com.twitter.flockdb.unit

import scala.collection.mutable
import com.twitter.gizzard.Future
import com.twitter.gizzard.scheduler._
import com.twitter.gizzard.nameserver.NameServer
import com.twitter.gizzard.shards.ShardInfo
import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.xrayspecs.Time
import com.twitter.xrayspecs.TimeConversions._
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

    val nameServer = mock[NameServer[Shard]]
    val uuidGenerator = mock[UuidGenerator]
    val forwardingManager = mock[ForwardingManager]
    val shard = mock[Shard]
    val scheduler = mock[PrioritizingJobScheduler[JsonJob]]
    val future = mock[Future]
    val copyFactory = mock[CopyJobFactory[Shard]]
    val flock = new FlockDB(new EdgesService(nameServer, forwardingManager, copyFactory, scheduler, future, future))

    "add" in {
      Time.freeze()
      val job = Add(bob, FOLLOWS, mary, Time.now.inMillis, Time.now, forwardingManager, uuidGenerator)
      expect {
        one(forwardingManager).find(0, FOLLOWS, Direction.Forward)
        one(scheduler).put(Priority.High.id, new JsonNestedJob(List(job)))
      }
      flock.execute(Select(bob, FOLLOWS, mary).add.toThrift)
    }

    "add_at" in {
      val job = Add(bob, FOLLOWS, mary, Time.now.inMillis, Time.now, forwardingManager, uuidGenerator)
      expect {
        one(forwardingManager).find(0, FOLLOWS, Direction.Forward)
        one(scheduler).put(Priority.High.id, new JsonNestedJob(List(job)))
      }
      flock.execute(Select(bob, FOLLOWS, mary).addAt(Time.now).toThrift)
    }

    "remove" in {
      Time.freeze()
      val job = Remove(bob, FOLLOWS, mary, Time.now.inMillis, Time.now, forwardingManager, uuidGenerator)
      expect {
        one(forwardingManager).find(0, FOLLOWS, Direction.Forward)
        one(scheduler).put(Priority.High.id, new JsonNestedJob(List(job)))
      }
      flock.execute(Select(bob, FOLLOWS, mary).remove.toThrift)
    }

    "remove_at" in {
      val job = Remove(bob, FOLLOWS, mary, Time.now.inMillis, Time.now, forwardingManager, uuidGenerator)
      expect {
        one(forwardingManager).find(0, FOLLOWS, Direction.Forward)
        one(scheduler).put(Priority.High.id, new JsonNestedJob(List(job)))
      }
      flock.execute(Select(bob, FOLLOWS, mary).removeAt(Time.now).toThrift)
    }

    "contains" in {
      expect {
        one(forwardingManager).find(bob, FOLLOWS, Direction.Forward) willReturn shard
        one(shard).get(bob, mary) willReturn Some(new Edge(bob, mary, 0, Time.now, 1, State.Normal))
      }
      flock.contains(bob, FOLLOWS, mary) must beTrue
    }
  }
}
