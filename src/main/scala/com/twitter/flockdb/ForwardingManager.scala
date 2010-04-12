package com.twitter.flockdb

import com.twitter.gizzard.nameserver.{Forwarding, NameServer}
import com.twitter.gizzard.shards.ShardException
import com.twitter.gizzard.thrift.conversions.Sequences._
import shards.Shard


class ForwardingManager(nameServer: NameServer[Shard]) {
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

  def findCurrentForwarding(tableId: List[Int], id: Long): Shard = {
    find(id, tableId(0), if (tableId(1) > 0) Direction.Forward else Direction.Backward)
  }
}
