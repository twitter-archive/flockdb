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

import com.twitter.util.Time
import org.specs.mock.{ClassMocker, JMocker}
import jobs.single._

object EdgeSpec extends ConfiguredSpecification with JMocker with ClassMocker  {
  val now = Time.fromSeconds(124)
  val source = 1
  val dest = 2
  val pos = 0
  val graph = 5
  val count = 0
  val forwardingManager = mock[ForwardingManager]

  "Edge" should {
    "becomes correct job" in {
      val edge = new Edge(source, dest, pos, now, count, State.Normal)
      edge.toJob(graph, forwardingManager, NoOpFilter) mustEqual new Single(source, graph, dest, pos, State.Normal, now, forwardingManager, OrderedUuidGenerator, NoOpFilter)
    }
  }
}
