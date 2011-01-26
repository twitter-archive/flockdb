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

import com.twitter.util.Time
import com.twitter.flockdb.jobs.single._
import org.specs.mock.{ClassMocker, JMocker}

object EdgeSpec extends ConfiguredSpecification with JMocker with ClassMocker  {
  val now = new Time(124)
  val source = 1
  val dest = 2
  val pos = 12345
  val graph = 5
  val count = 0
  val forwardingManager = mock[ForwardingManager]

  "Edge" should {
    "normal becomes single.Add" in {
      val edge = Edge(source, dest, pos, now, count, State.Normal)
      edge.toJob(graph, forwardingManager) mustEqual new Add(source, graph, dest, pos, now, forwardingManager, OrderedUuidGenerator)
    }

    "removed becomes single.Remove" in {
      val edge = Edge(source, dest, pos, now, count, State.Removed)
      edge.toJob(graph, forwardingManager) mustEqual new Remove(source, graph, dest, pos, now, forwardingManager, OrderedUuidGenerator)
    }

    "archived becomes single.Archive" in {
      val edge = Edge(source, dest, pos, now, count, State.Archived)
      edge.toJob(graph, forwardingManager) mustEqual new Archive(source, graph, dest, pos, now, forwardingManager, OrderedUuidGenerator)
    }
    "negative becomes single.Negate" in {
      val edge = Edge(source, dest, pos, now, count, State.Negative)
      edge.toJob(graph, forwardingManager) mustEqual new Negate(source, graph, dest, pos, now, forwardingManager, OrderedUuidGenerator)
    }

  }
}
