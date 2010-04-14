package com.twitter.flockdb

import com.twitter.querulous.query.SqlQueryFactory
import com.twitter.querulous.evaluator.StandardQueryEvaluatorFactory
import com.twitter.gizzard.nameserver.Forwarding
import com.twitter.gizzard.shards.{Busy, ShardInfo}


trait EdgesReset extends Reset {
  import Database._

  def reset(edges: Edges) {
    val queryEvaluatorFactory = new StandardQueryEvaluatorFactory(
      databaseFactory,
      new SqlQueryFactory)
    val queryEvaluator = queryEvaluatorFactory("localhost", config("edges.db_name"), config("db.username"), config("db.password"))
    for (graph <- (1 until 10)) {
      val forwardShardId = edges.nameServer.createShard(new ShardInfo("com.twitter.flockdb.SqlShard",
        "forward_" + graph, "localhost", "INT UNSIGNED", "INT UNSIGNED"))
      val backwardShardId = edges.nameServer.createShard(new ShardInfo("com.twitter.flockdb.SqlShard",
        "backward_" + graph, "localhost", "INT UNSIGNED", "INT UNSIGNED"))
      queryEvaluator.execute("DELETE FROM forward_" + graph + "_edges")
      queryEvaluator.execute("DELETE FROM forward_" + graph + "_metadata")
      queryEvaluator.execute("DELETE FROM backward_" + graph + "_edges")
      queryEvaluator.execute("DELETE FROM backward_" + graph + "_metadata")

      val replicatingForwardShardId = edges.nameServer.createShard(new ShardInfo("com.twitter.flockdb.ReplicatingShard",
        "replicating_forward_" + graph, "localhost", "", ""))
      val replicatingBackwardShardId = edges.nameServer.createShard(new ShardInfo("com.twitter.flockdb.ReplicatingShard",
        "replicating_backward_" + graph, "localhost", "", ""))
      edges.nameServer.addChildShard(replicatingForwardShardId, forwardShardId, 1)
      edges.nameServer.addChildShard(replicatingBackwardShardId, backwardShardId, 1)
      edges.nameServer.setForwarding(new Forwarding(graph, 0, replicatingForwardShardId))
      edges.nameServer.setForwarding(new Forwarding(-1 * graph, 0, replicatingBackwardShardId))
    }
    edges.nameServer.reload()
  }
}
