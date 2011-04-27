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

import shards.Shard
import com.twitter.gizzard.Stats

class SimpleQuery(shard: Shard, sourceId: Long, states: Seq[State]) extends Query {
  def sizeEstimate() = {
    Stats.transaction.record("Selecting counts from "+shard)
    shard.count(sourceId, states)
  }

  def selectWhereIn(page: Seq[Long]) = {
    Stats.transaction.record("Intersecting "+page.size+" ids from "+shard)
    shard.intersect(sourceId, states, page)
  }

  def selectPageByDestinationId(count: Int, cursor: Cursor) = {
    Stats.transaction.record("Selecting "+count+" destinationIds from "+shard)
    shard.selectByDestinationId(sourceId, states, count, cursor)
  }

  def selectPage(count: Int, cursor: Cursor) = {
    Stats.transaction.record("Selecting "+count+" edges from "+shard)
    shard.selectByPosition(sourceId, states, count, cursor)
  }

  override def toString =
    "<SimpleQuery sourceId="+sourceId+" states=("+states.map(_.name).mkString(",")+")>"
}
