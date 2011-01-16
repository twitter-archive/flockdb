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

import java.io.File
import org.specs.Specification
import com.twitter.gizzard.shards.{Busy, ShardId, ShardInfo}
import com.twitter.gizzard.nameserver.Forwarding
import com.twitter.gizzard.test.NameServerDatabase
import com.twitter.util.Eval
import com.twitter.querulous.evaluator.QueryEvaluatorFactory
import com.twitter.ostrich.W3CStats
import net.lag.logging.Logger
import net.lag.configgy.Configgy
import scala.collection.mutable

object MemoizedQueryEvaluators {
  val evaluators = mutable.Map[String,QueryEvaluatorFactory]()
}

abstract class ConfiguredSpecification extends Specification {
  lazy val config = {
    val c = Eval[flockdb.config.FlockDB](new File("config/test.scala"))
    try {
      c.logging()
      c
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
  }
}

abstract class IntegrationSpecification extends ConfiguredSpecification with NameServerDatabase {
  val (manager, nameServer, flock, jobScheduler) = {
    // XXX: Ostrich has a bug which causes a NPE when you pass in an empty array to W3CStats.
    // Remove this when we upgrade ostrich to a version that contains commit 71d07d32dcb76b029bdc11c519c867d7a2431cc2
    val f = new FlockDB(config, new W3CStats(Logger.get, Array("workaround")))
    (f.managerServer, f.nameServer, f.flockService, f.jobScheduler)
  }

  jobScheduler.start()

  def reset(config: flockdb.config.FlockDB) {
    materialize(config.nameServer)
    nameServer.rebuildSchema()
    nameServer.reload()

    val rootQueryEvaluator = config.edgesQueryEvaluator()(config.databaseConnection.withoutDatabase)
    //rootQueryEvaluator.execute("DROP DATABASE IF EXISTS " + config.databaseConnection.database)
    val queryEvaluator = config.edgesQueryEvaluator()(config.databaseConnection)

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
    println("reloaded")
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
