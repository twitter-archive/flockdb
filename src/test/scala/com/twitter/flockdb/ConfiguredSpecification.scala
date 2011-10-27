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
import com.twitter.gizzard.scheduler._
import com.twitter.gizzard.test.NameServerDatabase
import com.twitter.util.Eval
import com.twitter.querulous.evaluator.QueryEvaluatorFactory
import com.twitter.logging.Logger
import scala.collection.mutable
import com.twitter.flockdb

object MemoizedQueryEvaluators {
  val evaluators = mutable.Map[String,QueryEvaluatorFactory]()
}

object Config {
  val config = {
    val c = Eval[flockdb.config.FlockDB](new File("config/test.scala"))
    try {
      c.loggers.foreach { _() }
      c
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
  }
}

abstract class ConfiguredSpecification extends Specification {
  val config = Config.config
  def jsonMatching(list1: Iterable[JsonJob], list2: Iterable[JsonJob]) = {
    list1 must eventually(verify(l1 => { l1.map(_.toJson).sameElements(list2.map(_.toJson))}))
  }
}

abstract class IntegrationSpecification extends ConfiguredSpecification with NameServerDatabase {
  lazy val flock = {
    val f = new FlockDB(config)
    f.jobScheduler.start()
    f
  }

  lazy val flockService = flock.flockService

  def reset(config: flockdb.config.FlockDB) { reset(config, 1) }

  def reset(config: flockdb.config.FlockDB, count: Int) {
    materialize(config)
    flock.nameServer.reload()

    val rootQueryEvaluator = config.edgesQueryEvaluator()(config.databaseConnection.withoutDatabase)
    //rootQueryEvaluator.execute("DROP DATABASE IF EXISTS " + config.databaseConnection.database)
    val queryEvaluator = config.edgesQueryEvaluator()(config.databaseConnection)

    for (graph <- (1 until 10)) {
      Seq("forward", "backward").foreach { direction =>
        val tableId = if (direction == "forward") graph else graph * -1
        val replicatingShardId = ShardId("localhost", "replicating_" + direction + "_" + graph)
        flock.shardManager.createAndMaterializeShard(
          ShardInfo(replicatingShardId, "com.twitter.gizzard.shards.ReplicatingShard", "", "", Busy.Normal)
        )
        flock.shardManager.setForwarding(Forwarding(tableId, 0, replicatingShardId))

        for (sqlShardId <- (1 to count)) {
          val shardId = ShardId("localhost", direction + "_" + sqlShardId + "_" + graph)

          flock.shardManager.createAndMaterializeShard(ShardInfo(shardId,
            "com.twitter.flockdb.SqlShard", "INT UNSIGNED", "INT UNSIGNED", Busy.Normal))
          flock.shardManager.addLink(replicatingShardId, shardId, 1)

          queryEvaluator.execute("DELETE FROM " + shardId.tablePrefix + "_edges")
          queryEvaluator.execute("DELETE FROM " + shardId.tablePrefix + "_metadata")
        }
      }
    }

    flock.nameServer.reload()
  }

  def jobSchedulerMustDrain = {
    var last = flock.jobScheduler.size
    while(flock.jobScheduler.size > 0) {
      flock.jobScheduler.size must eventually(be_<(last))
      last = flock.jobScheduler.size
    }
    while(flock.jobScheduler.activeThreads > 0) {
      Thread.sleep(10)
    }
  }

  def reset(config: flockdb.config.FlockDB, db: String) {
    try {
      evaluator(config).execute("DROP DATABASE IF EXISTS " + db)
    } catch {
      case e =>
        e.printStackTrace()
        throw e
    }
  }
}
