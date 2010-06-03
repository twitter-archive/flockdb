package com.twitter.flockdb.test

import com.twitter.querulous.query.SqlQueryFactory
import com.twitter.querulous.evaluator.StandardQueryEvaluatorFactory
import net.lag.configgy.{ConfigMap, Configgy}
import com.twitter.gizzard.nameserver.Forwarding
import com.twitter.gizzard.shards.{Busy, ShardId, ShardInfo}
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
        val forwardShardId = ShardId("localhost", "forward_" + graph)
        val backwardShardId = ShardId("localhost", "backward_" + graph)

        flock.nameServer.createShard(ShardInfo(forwardShardId,
          "com.twitter.flockdb.SqlShard", "INT UNSIGNED", "INT UNSIGNED", Busy.Normal))
        flock.nameServer.createShard(ShardInfo(backwardShardId,
          "com.twitter.flockdb.SqlShard", "INT UNSIGNED", "INT UNSIGNED", Busy.Normal))
        queryEvaluator.execute("DELETE FROM forward_" + graph + "_edges")
        queryEvaluator.execute("DELETE FROM forward_" + graph + "_metadata")
        queryEvaluator.execute("DELETE FROM backward_" + graph + "_edges")
        queryEvaluator.execute("DELETE FROM backward_" + graph + "_metadata")

        val replicatingForwardShardId = ShardId("localhost", "replicating_forward_" + graph)
        val replicatingBackwardShardId = ShardId("localhost", "replicating_backward_" + graph)
        flock.nameServer.createShard(ShardInfo(replicatingForwardShardId,
          "com.twitter.gizzard.shards.ReplicatingShard", "", "", Busy.Normal))
        flock.nameServer.createShard(ShardInfo(replicatingBackwardShardId,
          "com.twitter.gizzard.shards.ReplicatingShard", "", "", Busy.Normal))
        flock.nameServer.addLink(replicatingForwardShardId, forwardShardId, 1)
        flock.nameServer.addLink(replicatingBackwardShardId, backwardShardId, 1)
        flock.nameServer.setForwarding(Forwarding(graph, 0, replicatingForwardShardId))
        flock.nameServer.setForwarding(Forwarding(-1 * graph, 0, replicatingBackwardShardId))
      }
      flock.nameServer.reload()
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
