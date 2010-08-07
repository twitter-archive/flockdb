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

package com.twitter.flockdb.shards

import java.util.Date
import java.io.DataOutputStream
import java.net.URI

import scala.collection.mutable.{HashMap, Map}
import com.twitter.flockdb.conversions.Edge.RichFlockEdge
import com.twitter.flockdb.conversions.Metadata.RichFlockMetadata

import com.twitter.gizzard.shards.{ShardInfo, ShardException, ShardFactory}
import com.twitter.xrayspecs.Time
import com.twitter.results.{Cursor, ResultWindow}
import com.twitter.elephantbird.util.TypeRef
import com.twitter.elephantbird.mapreduce.output.LzoThriftB64LineRecordWriter
import com.twitter.elephantbird.mapreduce.io.ThriftB64LineWritable

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import net.lag.configgy.ConfigMap
import net.lag.logging.Logger

import State._

sealed trait CopyState
case object NotCopying extends CopyState
case object Copying extends CopyState

class MemoizingHdfsBackupShardFactory(val hdfsBackupShardFactory: HdfsBackupShardFactory) extends ShardFactory[Shard]{
  val shardCache = new HashMap[ShardInfo, HdfsBackupShard]
  
  def instantiate(shardInfo: ShardInfo, weight: Int, children: Seq[Shard]) = {
    shardCache.get(shardInfo) match {
      case Some(shard) => shard
      case None => {
        val shard = hdfsBackupShardFactory.instantiate(shardInfo, weight, children)
        shardCache += (shardInfo -> shard)
        shard
      }
    }
  }
  
  def materialize(shardInfo: ShardInfo) = hdfsBackupShardFactory.materialize(shardInfo)
}

class HdfsBackupShardFactory extends ShardFactory[Shard] {
  def instantiate(shardInfo: ShardInfo, weight: Int, children: Seq[Shard]) = new HdfsBackupShard(shardInfo, weight, children)
  def materialize(shardInfo: ShardInfo) = ()
}

class HdfsBackupShard(val shardInfo: ShardInfo, val weight: Int, val children: Seq[Shard]) extends Shard {  
  private val UNSUPPORTED_ERROR = "Unsupported operation"
  
  val log = Logger.get(getClass.getName)
  
  private val path = shardInfo.hostname + "/" + shardInfo.tablePrefix.replace("_", "/")
  private val fs = FileSystem.get(URI.create(path), new Configuration)
  private val edgeThriftWritable = new ThriftB64LineWritable(new TypeRef[thrift.Edge] {})  
  private val metadataThriftWritable = new ThriftB64LineWritable(new TypeRef[thrift.Metadata] {})
  
  private var edgeWriter: LzoThriftB64LineRecordWriter[thrift.Edge] = null
  private var edgePath: String = null
  private var metadataWriter: LzoThriftB64LineRecordWriter[thrift.Metadata] = null
  private var metadataPath: String = null
  
  private var edgeCopyState: CopyState = NotCopying
  private var metadataCopyState: CopyState = NotCopying

  def startEdgeCopy() = {
    checkStateExecute(edgeCopyState, NotCopying, { () => 
      edgePath = path + "/edges"
      println("Starting edge copy")
      fs.delete(new Path(edgePath), true)
      edgeWriter = new LzoThriftB64LineRecordWriter[thrift.Edge](new TypeRef[thrift.Edge] {}, fs.create(new Path(edgePath + "/data")))
      edgeCopyState = Copying
    })
  }
  
  def writeCopies(edges: Seq[Edge]) = {
    checkStateExecute(edgeCopyState, Copying, { () => 
      edges foreach { edge => 
        println("Writing edge")
        val thriftEdge = new RichFlockEdge(edge).toThrift
        edgeThriftWritable.set(thriftEdge)
        edgeWriter.write(NullWritable get, edgeThriftWritable) 
      }
    })
  }
  
  def finishEdgeCopy() = {
    checkStateExecute(edgeCopyState, Copying, { () => 
      fs.create(new Path(edgePath + "/_job_finished")).close()
      println("Finished edge copy")
      edgeWriter.close(null)
      edgePath = null
      edgeCopyState = NotCopying
    })
  }
  
  def startMetadataCopy() = {
    checkStateExecute(metadataCopyState, NotCopying, { () =>
      metadataPath = path + "/metadata"
      fs.delete(new Path(metadataPath), true)
      metadataWriter = new LzoThriftB64LineRecordWriter[thrift.Metadata](new TypeRef[thrift.Metadata] {}, fs.create(new Path(metadataPath + "/data")))
      metadataCopyState = Copying
    })
  }

  def writeMetadata(metadata: Metadata) = {
    checkStateExecute(metadataCopyState, Copying, { () =>
      val thriftMetadata = new RichFlockMetadata(metadata).toThrift
      metadataThriftWritable.set(thriftMetadata)
      metadataWriter.write(NullWritable get, metadataThriftWritable)
    })
  }

  def finishMetadataCopy() = {
    checkStateExecute(metadataCopyState, Copying, { () =>
      fs.create(new Path(metadataPath + "/_job_finished")).close()
      metadataWriter.close(null)
      metadataPath = null      
      metadataCopyState = NotCopying
    })
  }
  
  def needsEdgeCopyRestart() = edgeCopyState == NotCopying

  def needsMetadataCopyRestart() = metadataCopyState == NotCopying
  
  private def checkStateExecute(current: CopyState, desired: CopyState, function: (() => Unit)) {  
    if (current == desired) 
      function()
    else
      throw new ShardException("Illegal HDFS backup shard state " + current + ", expected " + desired)
  }
  
  /**
  * Remaining functions are unsupported
  **/  
  def get(sourceId: Long, destinationId: Long) = throw new ShardException(UNSUPPORTED_ERROR)
  def getMetadata(sourceId: Long) = throw new ShardException(UNSUPPORTED_ERROR)
  def count(sourceId: Long, states: Seq[State]) = throw new ShardException(UNSUPPORTED_ERROR)
  def counts(sourceIds: Seq[Long], results: Map[Long, Int]) = throw new ShardException(UNSUPPORTED_ERROR)    

  def selectAll(cursor: (Cursor, Cursor), count: Int) = throw new ShardException(UNSUPPORTED_ERROR)
  def selectAllMetadata(cursor: Cursor, count: Int) = throw new ShardException(UNSUPPORTED_ERROR)
  def selectIncludingArchived(sourceId: Long, count: Int, cursor: Cursor) = throw new ShardException(UNSUPPORTED_ERROR)
  def selectByDestinationId(sourceId: Long, states: Seq[State], count: Int, cursor: Cursor) = throw new ShardException(UNSUPPORTED_ERROR)
  def selectByPosition(sourceId: Long, states: Seq[State], count: Int, cursor: Cursor) = throw new ShardException(UNSUPPORTED_ERROR)
  def selectEdges(sourceId: Long, states: Seq[State], count: Int, cursor: Cursor) = throw new ShardException(UNSUPPORTED_ERROR)

  def add(sourceId: Long, destinationId: Long, position: Long, updatedAt: Time) = throw new ShardException(UNSUPPORTED_ERROR)
  def add(sourceId: Long, updatedAt: Time) = throw new ShardException(UNSUPPORTED_ERROR)
  def updateMetadata(metadata: Metadata) = throw new ShardException(UNSUPPORTED_ERROR)

  def archive(sourceId: Long, destinationId: Long, position: Long, updatedAt: Time) = throw new ShardException(UNSUPPORTED_ERROR)
  def archive(sourceId: Long, updatedAt: Time) = throw new ShardException(UNSUPPORTED_ERROR)

  def remove(sourceId: Long, destinationId: Long, position: Long, updatedAt: Time) = throw new ShardException(UNSUPPORTED_ERROR)
  def remove(sourceId: Long, updatedAt: Time) = throw new ShardException(UNSUPPORTED_ERROR)

  def negate(sourceId: Long, destinationId: Long, position: Long, updatedAt: Time) = throw new ShardException(UNSUPPORTED_ERROR)
  def negate(sourceId: Long, updatedAt: Time) = throw new ShardException(UNSUPPORTED_ERROR)

  def withLock[A](sourceId: Long)(f: (Shard, Metadata) => A) = throw new ShardException(UNSUPPORTED_ERROR)
  
  def intersect(sourceId: Long, states: Seq[State], destinationIds: Seq[Long]) = throw new ShardException(UNSUPPORTED_ERROR)
  def intersectEdges(sourceId: Long, states: Seq[State], destinationIds: Seq[Long]) = throw new ShardException(UNSUPPORTED_ERROR)
}