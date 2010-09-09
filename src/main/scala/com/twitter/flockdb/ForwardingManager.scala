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

import scala.collection.{immutable, mutable}
import com.twitter.gizzard.nameserver.{Forwarding, NameServer}
import com.twitter.gizzard.shards.ShardException
import com.twitter.gizzard.thrift.conversions.Sequences._
import shards.Shard


case class NodePair(sourceId: Long, destinationId: Long)

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

  // returns a list of the NodePairs that failed their optimistic lock.
  // FIXME: may want to optimize the (frequent) case of one NodePair.
  def withOptimisticLocks(graphId: Int, nodePairs: Seq[NodePair])(f: (Shard, Shard, NodePair, State) => Unit): Seq[NodePair] = {
    val shardMap = mutable.Map.empty[Long, Shard]
    nodePairs.foreach { nodePair =>
      shardMap += (nodePair.sourceId -> find(nodePair.sourceId, graphId, Direction.Forward))
      shardMap += (nodePair.destinationId -> find(nodePair.destinationId, graphId, Direction.Backward))
    }

    def getState(stateMap: mutable.Map[Long, State], id: Long) = {
      stateMap.getOrElseUpdate(id, shardMap(id).getMetadata(id).map(_.state).getOrElse(State.Normal))
    }

    val initialStateMap = mutable.Map.empty[Long, State]
    nodePairs.foreach { nodePair =>
      val nodeState = getState(initialStateMap, nodePair.sourceId) max getState(initialStateMap, nodePair.destinationId)
      f(shardMap(nodePair.sourceId), shardMap(nodePair.destinationId), nodePair, nodeState)
    }

    val rv = new mutable.ListBuffer[NodePair]
    val afterStateMap = mutable.Map.empty[Long, State]
    nodePairs.foreach { nodePair =>
      if (getState(afterStateMap, nodePair.sourceId) != initialStateMap(nodePair.sourceId) ||
          getState(afterStateMap, nodePair.destinationId) != initialStateMap(nodePair.destinationId)) {
        rv += nodePair
      }
    }
    rv.toList
  }
}
