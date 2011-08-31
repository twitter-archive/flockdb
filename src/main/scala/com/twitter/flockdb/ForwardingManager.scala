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

import com.twitter.gizzard.nameserver.MultiForwarder
import com.twitter.gizzard.shards.{RoutingNode, ShardException}
import com.twitter.flockdb.shards.{Shard, ReadWriteShardAdapter}


class ForwardingManager(val forwarder: MultiForwarder[Shard]) {
  @throws(classOf[ShardException])
  def find(sourceId: Long, graphId: Int, direction: Direction): Shard = {
    new ReadWriteShardAdapter(findNode(sourceId, graphId, direction))
  }

  @throws(classOf[ShardException])
  def findNode(sourceId: Long, graphId: Int, direction: Direction)= {
    forwarder.find(translate(graphId, direction), sourceId)
  }

  private def translate(graphId: Int, direction: Direction) = {
    if (direction == Direction.Backward) -1 * graphId else graphId
  }
}
