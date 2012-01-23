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

import scala.collection.JavaConversions._
import scala.collection.mutable
import com.twitter.gizzard.thrift.conversions.ShardInfo._
import com.twitter.gizzard.scheduler.{JsonJob, PrioritizingJobScheduler}
import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.gizzard.shards.{ShardInfo, ShardId, Busy, RoutingNode}
import com.twitter.gizzard.nameserver.Forwarding
import com.twitter.util.Time
import com.twitter.util.TimeConversions._
import org.specs.util.{Duration => SpecsDuration}
import org.specs.mock.{ClassMocker, JMocker}
import com.twitter.flockdb
import com.twitter.flockdb.{Edge, Metadata}
import com.twitter.flockdb.config.{FlockDB => FlockDBConfig}
import shards.{Shard, SqlShard}
import thrift._

class CopySpec extends IntegrationSpecification {
  "Copy" should {
    val sourceShardId = ShardId("localhost", "copy_test1")
    val destinationShardId = ShardId("localhost", "copy_test2")
    val shard3Id = ShardId("localhost", "copy_test3")
    val sourceShardInfo = new ShardInfo(sourceShardId,
            "com.twitter.flockdb.SqlShard", "INT UNSIGNED", "INT UNSIGNED", Busy.Normal)
    val destinationShardInfo = new ShardInfo(destinationShardId,
            "com.twitter.flockdb.SqlShard", "INT UNSIGNED", "INT UNSIGNED", Busy.Normal)
    val shard3Info = new ShardInfo(shard3Id,
          "com.twitter.flockdb.SqlShard", "INT UNSIGNED", "INT UNSIGNED", Busy.Normal)
    val time = Time.now

    doBefore {
      val queryEvaluator = config.edgesQueryEvaluator()(config.databaseConnection)

      queryEvaluator.execute("DROP TABLE IF EXISTS copy_test1_edges")()
      queryEvaluator.execute("DROP TABLE IF EXISTS copy_test1_metadata")()
      queryEvaluator.execute("DROP TABLE IF EXISTS copy_test2_edges")()
      queryEvaluator.execute("DROP TABLE IF EXISTS copy_test2_metadata")()
      queryEvaluator.execute("DROP TABLE IF EXISTS copy_test3_edges")()
      queryEvaluator.execute("DROP TABLE IF EXISTS copy_test3_metadata")()
      flock.nameServer.reload()
      flock.shardManager.createAndMaterializeShard(sourceShardInfo)
      flock.shardManager.createAndMaterializeShard(destinationShardInfo)
      flock.shardManager.createAndMaterializeShard(shard3Info)
      flock.shardManager.setForwarding(new Forwarding(0, Long.MinValue, sourceShardInfo.id))

    }

    doAfter {
       val queryEvaluator = config.edgesQueryEvaluator()(config.databaseConnection)
       queryEvaluator.execute("DROP TABLE IF EXISTS copy_test1_edges")()
       queryEvaluator.execute("DROP TABLE IF EXISTS copy_test1_metadata")()
       queryEvaluator.execute("DROP TABLE IF EXISTS copy_test2_edges")()
       queryEvaluator.execute("DROP TABLE IF EXISTS copy_test2_metadata")()
       queryEvaluator.execute("DROP TABLE IF EXISTS copy_test3_edges")()
       queryEvaluator.execute("DROP TABLE IF EXISTS copy_test3_metadata")()
    }


    def writeEdges(shard: RoutingNode[Shard], num: Int, start: Int, step: Int, outdated: Boolean, state: State = State.Normal) {
      val edges = for (id <- start to num by step) yield {
        Edge(1L, id.toLong, id.toLong, (if (outdated) time-1.seconds else time), 0, state)
      }

      shard.write.foreach { _.writeCopies(edges)() }
    }

    def getEdges(shard: RoutingNode[Shard], num: Int) {
      shard.read.any { _.count(1L, Seq(State.Normal))() } mustEqual num
    }

    def validateEdges(shards: Seq[RoutingNode[Shard]], num: Int) {
      playScheduledJobs()

      val shardsEdges = shards map { _.read.any { _.selectAll((Cursor.Start, Cursor.Start), 2*num)()._1 } }
      shardsEdges.foreach { _.length mustEqual num }

      for (idx <- 0 until num) {
        val head :: others = shardsEdges

        others foreach { edges =>
          head zip edges foreach { case (a, b) =>
            a mustEqual b
            b.updatedAt.inSeconds mustEqual time.inSeconds
          }
        }
      }
    }

    "do nothing on equivalent shards" in {
      val numData = 100
      val shard1 = flock.nameServer.findShardById[Shard](sourceShardId)
      val shard2 = flock.nameServer.findShardById[Shard](destinationShardId)
      writeEdges(shard1, numData, 1, 1, false)
      writeEdges(shard2, numData, 1, 1, false)

      flock.managerServer.copy_shard(Seq(sourceShardInfo.toThrift.id, destinationShardInfo.toThrift.id))

      validateEdges(Seq(shard1, shard2), numData)
    }

    "copy" in {
      val numData = 100
      val shard1 = flock.nameServer.findShardById[Shard](sourceShardId)
      val shard2 = flock.nameServer.findShardById[Shard](destinationShardId)
      writeEdges(shard1, numData, 1, 1, false)

      flock.managerServer.copy_shard(Seq(sourceShardInfo.toThrift.id, destinationShardInfo.toThrift.id))

      validateEdges(Seq(shard1, shard2), numData)
    }

    "repair by merging" in {
      val numData = 100
      val shard1 = flock.nameServer.findShardById[Shard](sourceShardId)
      val shard2 = flock.nameServer.findShardById[Shard](destinationShardId)
      writeEdges(shard1, numData, 1, 2, false)
      writeEdges(shard2, numData, 2, 2, false)

      flock.managerServer.copy_shard(Seq(sourceShardInfo.toThrift.id, destinationShardInfo.toThrift.id))

      validateEdges(Seq(shard1, shard2), numData)
    }

    "repair and fill out of date" in {
      val numData = 100

      val shard1 = flock.nameServer.findShardById[Shard](sourceShardId)
      val shard2 = flock.nameServer.findShardById[Shard](destinationShardId)
      val shard3 = flock.nameServer.findShardById[Shard](shard3Id)

      writeEdges(shard1, numData, 1, 2, false)
      writeEdges(shard2, numData/2, 2, 2, false)
      writeEdges(shard2, numData/2, 1, 2, true)
      writeEdges(shard2, numData, numData/2, 1, false)
      writeEdges(shard3, numData, 1, 3, true, State.Archived)

      shard1.write.foreach { _.writeMetadata(Metadata(1L, State.Normal, time))() }
      shard2.write.foreach { _.writeMetadata(Metadata(1L, State.Normal, time))() }
      shard3.write.foreach { _.writeMetadata(Metadata(1L, State.Archived, (time - 1.seconds)) )() }

      flock.managerServer.copy_shard(Seq(sourceShardInfo.toThrift.id, destinationShardInfo.toThrift.id, shard3Info.toThrift.id))
      validateEdges(Seq(shard1, shard2, shard3), numData)
    }
  }
}
