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
package queries

import net.lag.configgy.Configgy
import shards.Shard

class SimpleQuery(shard: Shard, sourceId: Long, states: Seq[State]) extends Query {
  val config = Configgy.config

  def sizeEstimate() = shard.count(sourceId, states)

  def selectWhereIn(page: Seq[Long]) = shard.intersect(sourceId, states, page)

  def selectPageByDestinationId(count: Int, cursor: Cursor) = {
    shard.selectByDestinationId(sourceId, states, count, cursor)
  }

  def selectPage(count: Int, cursor: Cursor) = {
    shard.selectByPosition(sourceId, states, count, cursor)
  }
}
