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

package com.twitter.flockdb.integration

import com.twitter.gizzard.scheduler.{JsonJob, PrioritizingJobScheduler}
import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.gizzard.shards.{ShardInfo, ShardId, Busy}
import com.twitter.gizzard.nameserver.Forwarding
import com.twitter.util.Time
import com.twitter.flockdb.{SelectQuery, Metadata}
import com.twitter.util.TimeConversions._
import org.specs.mock.{ClassMocker, JMocker}
import jobs.multi.{Archive, RemoveAll, Unarchive}
import jobs.single.{Add, Remove}
import shards.{Shard, SqlShard}
import thrift._

class BlackHoleLockingRegressionSpec extends IntegrationSpecification {
  override def reset(config: flockdb.config.FlockDB, name: String) {
    materialize(config.nameServer)
    nameServer.rebuildSchema()
    nameServer.reload()

    val rootQueryEvaluator = config.edgesQueryEvaluator()(config.databaseConnection.withoutDatabase)
    //rootQueryEvaluator.execute("DROP DATABASE IF EXISTS " + config.databaseConnection.database)
    val queryEvaluator = config.edgesQueryEvaluator()(config.databaseConnection)

    for (graph <- (1 until 10)) {
      Seq("forward", "backward").foreach { direction =>
        val tableId = if (direction == "forward") graph else graph * -1
        if (direction == "forward") {
          val shardId1 = ShardId("localhost", direction + "_" + graph + "_a")
          val shardId2 = ShardId("localhost", direction + "_" + graph + "_b")
          val replicatingShardId = ShardId("localhost", "replicating_" + direction + "_" + graph)

          nameServer.createShard(ShardInfo(shardId1,
            "com.twitter.flockdb.SqlShard", "INT UNSIGNED", "INT UNSIGNED", Busy.Normal))
          nameServer.createShard(ShardInfo(shardId2,
            "com.twitter.flockdb.SqlShard", "INT UNSIGNED", "INT UNSIGNED", Busy.Normal))
          nameServer.createShard(ShardInfo(replicatingShardId,
            "com.twitter.gizzard.shards.ReplicatingShard", "", "", Busy.Normal))
          nameServer.addLink(replicatingShardId, shardId1, 1)
          nameServer.addLink(replicatingShardId, shardId2, 1)
          nameServer.setForwarding(Forwarding(tableId, 0, replicatingShardId))

          queryEvaluator.execute("DELETE FROM " + direction + "_" + graph + "_a_edges")
          queryEvaluator.execute("DELETE FROM " + direction + "_" + graph + "_a_metadata")
          queryEvaluator.execute("DELETE FROM " + direction + "_" + graph + "_b_edges")
          queryEvaluator.execute("DELETE FROM " + direction + "_" + graph + "_b_metadata")
        } else {
          val shardId1 = ShardId("localhost", direction + "_" + graph + "_a")
          val shardId2 = ShardId("localhost", direction + "_" + graph + "_b")
          nameServer.createShard(ShardInfo(shardId1,
            name, "", "", Busy.Normal))
          nameServer.createShard(ShardInfo(shardId2,
            "com.twitter.flockdb.SqlShard", "INT UNSIGNED", "INT UNSIGNED", Busy.Normal))

          nameServer.addLink(shardId1, shardId2, 1)
          nameServer.setForwarding(Forwarding(tableId, 0, shardId1))
        }
      }
    }

    nameServer.reload()
  }

  val alice = 1L
  val FOLLOWS = 1
  val pageSize = 100

  def alicesFollowings() = {
    val term = new QueryTerm(alice, FOLLOWS, true)
    term.setState_ids(List[Int](State.Normal.id).toJavaList)
    val query = new EdgeQuery(term, new Page(pageSize, Cursor.Start.position))
    val resultsList = flock.select_edges(List[EdgeQuery](query).toJavaList).toList
    resultsList.size mustEqual 1
    resultsList(0).edges
  }

  "select results" should {
    "black hole" in {
      reset(config, "com.twitter.gizzard.shards.BlackHoleShard")  // I don't know why this isn't working in doBefore

      for(i <- 0 until 10) {
        flock.execute(Select(alice, FOLLOWS, i).add.toThrift)
      }

      alicesFollowings.size must eventually(be(10))
    }
  }

  "select results" should {
    "read-only" in {
      reset(config, "com.twitter.gizzard.shards.ReadOnlyShard")  // I don't know why this isn't working in doBefore

      for(i <- 0 until 10) {
        flock.execute(Select(alice, FOLLOWS, i).add.toThrift)
      }

      val scheduler = jobScheduler(flockdb.Priority.High.id)
      val errors = scheduler.errorQueue
      errors.size must eventually(be(10))
    }
  }

  "select results" should {
    "write-only" in {
      reset(config, "com.twitter.gizzard.shards.WriteOnlyShard")  // I don't know why this isn't working in doBefore

      for(i <- 0 until 10) {
        flock.execute(Select(alice, FOLLOWS, i).add.toThrift)
      }

      val scheduler = jobScheduler(flockdb.Priority.High.id)
      val errors = scheduler.errorQueue
      errors.size must eventually(be(10))
    }
  }

}

