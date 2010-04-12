package com.twitter.flockdb.unit

import scala.collection.{immutable, mutable}
import com.twitter.gizzard.shards
import com.twitter.ostrich.{StatsProvider, TimingStat}
import com.twitter.xrayspecs.Time
import com.twitter.xrayspecs.TimeConversions._
import org.specs.mock.{JMocker, ClassMocker}
import org.specs.Specification
import flockdb.shards.{ReadWriteShard, Shard, StatsCollectingShard}


object StatsCollectingShardSpec extends Specification with JMocker with ClassMocker {
  "StatsCollectingShard" should {
    Time.freeze()
    val duration = 1.second
    val alice = 1L
    val bob = 2L

    val shard = mock[Shard]
    val testShardInfo = new shards.ShardInfo("", "table_prefix", "hostname", "", "", shards.Busy.Normal, 1234)
    var readWriteShard = new ReadWriteShard {
      val shardInfo = testShardInfo
      val weight = 1
      val children = Nil

      def readOperation[A](f: Shard => A) = {
        Time.advance(duration)
        f(shard)
      }

      def writeOperation[A](f: Shard => A) = {
        Time.advance(duration)
        f(shard)
      }
    }
    val stats = new StatsProvider {
      var timing = new mutable.HashMap[String, Int]
      def addTiming(name: String, duration: Int) = {
        timing += (name -> duration)
        duration
      }
      def addTiming(name: String, timingStat: TimingStat) = 0
      def incr(name: String, count: Int): Long = count.toLong
      def getCounterStats(reset: Boolean) = immutable.Map.empty
      def getTimingStats(reset: Boolean) = immutable.Map.empty
      def clearAll() = ()
    }
    var statsCollectingShard: StatsCollectingShard = null

    doBefore {
      statsCollectingShard = new StatsCollectingShard(testShardInfo, 1, List(readWriteShard), stats)
    }

    "collect stats" >> {
      expect {
        one(shard).get(alice, bob)
        one(shard).add(alice, bob, 1, Time.now + duration)
      }

      statsCollectingShard.get(alice, bob)
      statsCollectingShard.add(alice, bob, 1, Time.now)
      stats.timing("shard-hostname-read") mustEqual duration.inMillis
      stats.timing("shard-hostname-write") mustEqual duration.inMillis
    }
  }
}
