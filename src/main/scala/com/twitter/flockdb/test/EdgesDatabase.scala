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

package com.twitter.flockdb.test

import com.twitter.ostrich.W3CStats
import net.lag.logging.Logger
import com.twitter.querulous.query.SqlQueryFactory
import com.twitter.querulous.evaluator.StandardQueryEvaluatorFactory
import net.lag.configgy.{ConfigMap, Configgy}
import com.twitter.gizzard.nameserver.Forwarding
import com.twitter.gizzard.shards.{Busy, ShardId, ShardInfo}
import com.twitter.gizzard.test.NameServerDatabase


trait EdgesDatabase extends NameServerDatabase {
  def reset(config: flockdb.config.FlockDB) {
    try {
      val nameServer     = new FlockDB(config, new W3CStats(Logger.get, Array())).nameServer
      val queryEvaluator = evaluator(config.nameServer)

      materialize(config.nameServer)
      nameServer.rebuildSchema()
      queryEvaluator.execute("DROP DATABASE IF EXISTS " + config.databaseConnection.database)

      nameServer.reload()

      for (graph <- (1 until 10)) {
        Seq("forward", "backward").foreach { direction =>
          val tableId = if (direction == "forward") graph else graph * -1
          val shardId = ShardId("localhost", direction + "_" + graph)
          val replicatingShardId = ShardId("localhost", "replicating_" + direction + "_" + graph)

          nameServer.createShard(ShardInfo(shardId,
            "com.twitter.flockdb.SqlShard", "INT UNSIGNED", "INT UNSIGNED", Busy.Normal))
          nameServer.createShard(ShardInfo(replicatingShardId,
            "com.twitter.gizzard.shards.ReplicatingShard", "", "", Busy.Normal))
          nameServer.addLink(replicatingShardId, shardId, 1)
          nameServer.setForwarding(Forwarding(tableId, 0, replicatingShardId))

          queryEvaluator.execute("DELETE FROM " + direction + "_" + graph + "_edges")
          queryEvaluator.execute("DELETE FROM " + direction + "_" + graph + "_metadata")
        }
      }

      nameServer.reload()
    } catch {
      case e =>
        e.printStackTrace()
        throw e
    }
  }

  def reset(config: flockdb.config.FlockDB, db: String) {
    try {
      evaluator(config.nameServer).execute("DROP DATABASE IF EXISTS " + db)
    } catch {
      case e =>
        e.printStackTrace()
        throw e
    }
  }
}
