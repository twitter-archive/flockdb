package com.twitter.flockdb.shards

import com.twitter.gizzard.Future
import com.twitter.gizzard.shards
import com.twitter.gizzard.nameserver.LoadBalancer
import net.lag.logging.ThrottledLogger
import scala.util.Random



class ReplicatingShardFactory(log: ThrottledLogger[String], future: Future) extends shards.ShardFactory[Shard] {
  def instantiate(shardInfo: shards.ShardInfo, weight: Int, replicas: Seq[Shard]) =
    new ReplicatingShard(shardInfo, weight, replicas, log, future)
  def materialize(shardInfo: shards.ShardInfo) = ()
}

class ReplicatingShard(shardInfo: shards.ShardInfo, weight: Int, replicas: Seq[Shard],
                       log: ThrottledLogger[String], future: Future)
  extends shards.ReplicatingShard[Shard](shardInfo, weight, replicas, new LoadBalancer(replicas), log, future)
  with ReadWriteShard {

  override def withLock[A](sourceId: Long)(f: (Shard, Metadata) => A) = {
    val lockServer = replicas.first.asInstanceOf[Shard]
    val rest = replicas.drop(1)
    lockServer.withLock(sourceId) { (lock, metadata) =>
      f(new ReplicatingShard(shardInfo, weight, List(lock) ++ rest, log, future), metadata)
    }
  }
}
