package com.twitter.flockdb.test

import com.twitter.querulous.query.SqlQueryFactory
import com.twitter.querulous.evaluator.StandardQueryEvaluatorFactory
import net.lag.configgy.{ConfigMap, Configgy}
import com.twitter.gizzard.nameserver.Forwarding
import com.twitter.gizzard.shards.{Busy, ShardInfo}
import com.twitter.gizzard.test.NameServerDatabase


trait EdgesDatabase extends NameServerDatabase {
  def reset(flock: FlockDB) {
    try {
      reset(Configgy.config.configMap("edges"))
      val config = Configgy.config.configMap("db")
      config.update("database", Configgy.config("edges.db_name"))
      config.update("hostname", "localhost")
      val queryEvaluator = evaluator(config)

      for (graph <- (1 until 10)) {
        val forwardShardId = flock.edges.nameServer.createShard(new ShardInfo("com.twitter.flockdb.SqlShard",
          "forward_" + graph, "localhost", "INT UNSIGNED", "INT UNSIGNED"))
        val backwardShardId = flock.edges.nameServer.createShard(new ShardInfo("com.twitter.flockdb.SqlShard",
          "backward_" + graph, "localhost", "INT UNSIGNED", "INT UNSIGNED"))
        queryEvaluator.execute("DELETE FROM forward_" + graph + "_edges")
        queryEvaluator.execute("DELETE FROM forward_" + graph + "_metadata")
        queryEvaluator.execute("DELETE FROM backward_" + graph + "_edges")
        queryEvaluator.execute("DELETE FROM backward_" + graph + "_metadata")

        val replicatingForwardShardId = flock.edges.nameServer.createShard(new ShardInfo("com.twitter.gizzard.shards.ReplicatingShard",
          "replicating_forward_" + graph, "localhost", "", ""))
        val replicatingBackwardShardId = flock.edges.nameServer.createShard(new ShardInfo("com.twitter.gizzard.shards.ReplicatingShard",
          "replicating_backward_" + graph, "localhost", "", ""))
        flock.edges.nameServer.addChildShard(replicatingForwardShardId, forwardShardId, 1)
        flock.edges.nameServer.addChildShard(replicatingBackwardShardId, backwardShardId, 1)
        flock.edges.nameServer.setForwarding(new Forwarding(graph, 0, replicatingForwardShardId))
        flock.edges.nameServer.setForwarding(new Forwarding(-1 * graph, 0, replicatingBackwardShardId))
      }
      flock.edges.nameServer.reload()
    } catch {
      case e =>
        e.printStackTrace()
        throw e
    }
  }

  def reset(config: ConfigMap, db: String) {
    try {
      rootEvaluator(config).execute("DROP DATABASE IF EXISTS " + db)
    } catch {
      case e =>
        e.printStackTrace()
        throw e
    }
  }
}
