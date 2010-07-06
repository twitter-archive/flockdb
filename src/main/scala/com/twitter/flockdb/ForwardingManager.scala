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

import com.twitter.gizzard.nameserver.{Forwarding, NameServer}
import com.twitter.gizzard.shards.ShardException
import com.twitter.gizzard.thrift.conversions.Sequences._
import shards.Shard


class ForwardingManager(nameServer: NameServer[Shard]) {
  @throws(classOf[ShardException])
  def find(sourceId: Long, graphId: Int, direction: Direction) = {
    nameServer.findCurrentForwarding(translate(graphId, direction), sourceId)
  }

  private def translate(graphId: Int, direction: Direction) = {
    if (direction == Direction.Backward) {
      -1 * graphId
    } else {
      graphId
    }
  }

  @throws(classOf[ShardException])
  def findCurrentForwarding(tableId: List[Int], id: Long): Shard = {
    find(id, tableId(0), if (tableId(1) > 0) Direction.Forward else Direction.Backward)
  }
}
