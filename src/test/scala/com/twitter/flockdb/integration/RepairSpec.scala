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
package integration

import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.ostrich.Stats
import com.twitter.util.Time
import com.twitter.util.TimeConversions._
import conversions.ExecuteOperations._
import conversions.SelectOperation._
import com.twitter.gizzard.thrift._
import com.twitter.gizzard.shards.{Busy, ShardId, ShardInfo}
import com.twitter.gizzard.nameserver.Forwarding
import shards.{SqlShard}
import jobs._

class RepairSpec extends IntegrationSpecification {

  "Repair" should {
    doBefore {
      reset(config, 3)
    }

    val replicatingShardId = ShardId("localhost", "replicating_forward_1")
    val (shard1id, shard2id, shard3id) = (ShardId("localhost", "forward_1_1"), ShardId("localhost", "forward_2_1"), ShardId("localhost", "forward_3_1"))
    lazy val shard1 = nameServer.findShardById(shard1id)
    lazy val shard2 = nameServer.findShardById(shard2id)
    lazy val shard3 = nameServer.findShardById(shard3id)

    "differing shards should become the same" in {
      shard1.add(1L, 2L, 1L, Time.now) // same
      shard2.add(1L, 2L, 1L, Time.now)

      shard1.archive(2L, 1L, 2L, Time.now) // one archived, one normal
      shard2.add(2L, 1L, 2L, Time.now)
      shard3.add(2L, 1L, 2L, Time.now)

      shard1.add(1L, 3L, 3L, Time.now) // only on one shard
      shard3.archive(1L, 3L, 3L, Time.now)

      shard2.add(1L, 4L, 4L, Time.now)  // only on two shard

      shard3.negate(3L, 1L, 5L, Time.now)  // only on two shard

      // bulk
      shard1.add(5L, 2L, 1L, Time.now) // same
      shard1.add(6L, 2L, 1L, Time.now) // same
      shard1.add(7L, 2L, 1L, Time.now) // same
      shard1.add(8L, 2L, 1L, Time.now) // same
      shard1.add(9L, 2L, 1L, Time.now) // same
      shard1.add(10L, 2L, 1L, Time.now) // same



      val list = new java.util.ArrayList[com.twitter.gizzard.thrift.ShardId]
      list.add(new com.twitter.gizzard.thrift.ShardId(shard1id.hostname, shard1id.tablePrefix))
      list.add(new com.twitter.gizzard.thrift.ShardId(shard2id.hostname, shard2id.tablePrefix))
      list.add(new com.twitter.gizzard.thrift.ShardId(shard3id.hostname, shard3id.tablePrefix))
      manager.repair_shard(list)
      def listElemenets(list: Seq[Edge]) = {
        list.map((e) => (e.sourceId, e.destinationId, e.state))
      }

      listElemenets(shard1.selectAll(Repair.START, Repair.COUNT)._1) must eventually(
        verify(s => s sameElements listElemenets(shard2.selectAll(Repair.START, Repair.COUNT)._1)))
      listElemenets(shard1.selectAll(Repair.START, Repair.COUNT)._1) must eventually(
        verify(s => s sameElements listElemenets(shard3.selectAll(Repair.START, Repair.COUNT)._1)))
      listElemenets(shard2.selectAll(Repair.START, Repair.COUNT)._1) must eventually(
        verify(s => s sameElements listElemenets(shard3.selectAll(Repair.START, Repair.COUNT)._1)))
    }
  }
}
