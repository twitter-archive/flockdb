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
package shards

import collection.mutable


class MultiShard(forwardingManager: ForwardingManager, sourceIds: Seq[Long], graphId: Int, direction: Direction) {
  private val shards = new mutable.HashMap[Shard, mutable.ArrayBuffer[Long]]
  for (id <- sourceIds) {
    val shard = forwardingManager.find(id, graphId, direction)
    shards.getOrElseUpdate(shard, new mutable.ArrayBuffer[Long]) += id
  }

  def counts = {
    val results = new mutable.HashMap[Long, Int]
    for ((shard, ids) <- shards) shard.counts(ids, results)
    sourceIds.map { results.getOrElse(_, 0) }
  }
}
