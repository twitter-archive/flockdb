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

import com.twitter.util.Duration
import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.gizzard.Stats
import shards.Shard

class WhereInQuery(shard: Shard, sourceId: Long, states: Seq[State], destinationIds: Seq[Long]) extends SimpleQueryNode {

  def sizeEstimate() = destinationIds.size

  def selectWhereIn(page: Seq[Long]) = {
    val intersection = (Set(destinationIds: _*) intersect Set(page: _*)).toSeq
    Stats.transaction.record("Intersecting "+intersection.size+" ids from "+shard)
    time(shard.intersect(sourceId, states, intersection))
  }

  def selectPageByDestinationId(count: Int, cursor: Cursor) = {
    Stats.transaction.record("Selecting "+count+" edges from an intersection of "+destinationIds.size+" ids")
    val results = time(shard.intersect(sourceId, states, destinationIds))
    Stats.transaction.record("Selected "+results.size+" rows.")
    new ResultWindow(results.map(result => (result, Cursor(result))), count, cursor)
  }

  def selectPage(count: Int, cursor: Cursor) = selectPageByDestinationId(count, cursor)

  override def toString = {
    "<WhereInQuery sourceId="+sourceId+" states=("+states.map(_.name).mkString(",")+") destIds=("+destinationIds.mkString(",")+")"+duration.map(" time="+_.inMillis).mkString+">"
  }
}
