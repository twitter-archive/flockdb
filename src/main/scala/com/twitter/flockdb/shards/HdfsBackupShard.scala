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
case object BeforeCopy extends CopyState
case object Copying extends CopyState
case object AfterCopy extends CopyState

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
  private val READS_ERROR = "Read operations not supported"
  private val INDIVIDUAL_WRITES_ERROR = "Individual writes not supported"
  private val UPDATES_ERROR = "Updates not supported"
  private val LOCKS_ERROR = "Shard locking not supported"
  
  val log_ = Logger.get(getClass.getName)
  
  private val path_ = shardInfo.hostname + "/" + shardInfo.tablePrefix
  private val fs_ = FileSystem.get(URI.create(path_), new Configuration)
  private val edgeThriftWritable_ = new ThriftB64LineWritable(new TypeRef[thrift.Edge] {})  
  private val metadataThriftWritable_ = new ThriftB64LineWritable(new TypeRef[thrift.Metadata] {})
  
  private var edgeWriter_ : LzoThriftB64LineRecordWriter[thrift.Edge] = null
  private var edgePath_ : String = null
  private var metadataWriter_ : LzoThriftB64LineRecordWriter[thrift.Metadata] = null
  private var metadataPath_ : String = null
  
  private var edgeCopyState_ : CopyState = BeforeCopy
  private var metadataCopyState_ : CopyState = BeforeCopy

  def startEdgeCopy() = {
    checkStateExecute(edgeCopyState_, BeforeCopy, { () => 
      edgePath_ = path_ + "/edges"
      edgeWriter_ = new LzoThriftB64LineRecordWriter[thrift.Edge](new TypeRef[thrift.Edge] {}, fs_.create(new Path(edgePath_ + "/data")))
      edgeCopyState_ = Copying
    })
  }
  
  def writeCopies(edges: Seq[Edge]) = {
    checkStateExecute(edgeCopyState_, Copying, { () => 
      edges foreach { edge => 
        val thriftEdge = new com.twitter.flockdb.conversions.Edge.RichFlockEdge(edge).toThrift
        edgeThriftWritable_.set(thriftEdge)
        edgeWriter_.write(NullWritable get, edgeThriftWritable_) 
      }
    })
  }
  
  def finishEdgeCopy() = {
    checkStateExecute(edgeCopyState_, Copying, { () => 
      fs_.create(new Path(edgePath_ + "/_job_finished")).close()
      edgeWriter_.close(null)
      edgePath_ = null      
      edgeCopyState_ = AfterCopy
    })  
  }
  
  def startMetadataCopy() = {
    checkStateExecute(metadataCopyState_, BeforeCopy, { () => 
      metadataPath_ = path_ + "/metadata"
      metadataWriter_ = new LzoThriftB64LineRecordWriter[thrift.Metadata](new TypeRef[thrift.Metadata] {}, fs_.create(new Path(metadataPath_ + "/data")))
      metadataCopyState_ = Copying
    })
  }

  def writeMetadata(metadata: Metadata) = {
    checkStateExecute(metadataCopyState_, Copying, { () => 
      val thriftMetadata = new com.twitter.flockdb.conversions.Metadata.RichFlockMetadata(metadata).toThrift
      metadataThriftWritable_.set(thriftMetadata)
      metadataWriter_.write(NullWritable get, metadataThriftWritable_)
    })
  }

  def finishMetadataCopy() = {
    checkStateExecute(metadataCopyState_, Copying, { () =>
      fs_.create(new Path(metadataPath_ + "/_job_finished")).close()
      metadataWriter_.close(null)
      metadataPath_ = null      
      metadataCopyState_ = AfterCopy
    })
  }
  
  private def checkStateExecute(current: CopyState, desired: CopyState, function : (() => Unit)) {  
    if (current == desired) 
      function()
    else
      throw new ShardException("Illegal HDFS backup shard state " + current + ", expected " + desired)
  }
  
  /**
  * Remaining functions are unsupported
  **/  
  def get(sourceId: Long, destinationId: Long) = throw new ShardException(READS_ERROR)
  def getMetadata(sourceId: Long) = throw new ShardException(READS_ERROR)
  def count(sourceId: Long, states: Seq[State]) = throw new ShardException(READS_ERROR)
  def counts(sourceIds: Seq[Long], results: Map[Long, Int]) = throw new ShardException(READS_ERROR)    

  def selectAll(cursor: (Cursor, Cursor), count: Int) = throw new ShardException(READS_ERROR)
  def selectAllMetadata(cursor: Cursor, count: Int) = throw new ShardException(READS_ERROR)
  def selectIncludingArchived(sourceId: Long, count: Int, cursor: Cursor) = throw new ShardException(READS_ERROR)
  def selectByDestinationId(sourceId: Long, states: Seq[State], count: Int, cursor: Cursor) = throw new ShardException(READS_ERROR)
  def selectByPosition(sourceId: Long, states: Seq[State], count: Int, cursor: Cursor) = throw new ShardException(READS_ERROR)
  def selectEdges(sourceId: Long, states: Seq[State], count: Int, cursor: Cursor) = throw new ShardException(READS_ERROR)

  def add(sourceId: Long, destinationId: Long, position: Long, updatedAt: Time) = throw new ShardException(INDIVIDUAL_WRITES_ERROR)
  def add(sourceId: Long, updatedAt: Time) = throw new ShardException(INDIVIDUAL_WRITES_ERROR)
  def updateMetadata(metadata: Metadata) = throw new ShardException(UPDATES_ERROR)

  def archive(sourceId: Long, destinationId: Long, position: Long, updatedAt: Time) = throw new ShardException(UPDATES_ERROR)
  def archive(sourceId: Long, updatedAt: Time) = throw new ShardException(UPDATES_ERROR)

  def remove(sourceId: Long, destinationId: Long, position: Long, updatedAt: Time) = throw new ShardException(UPDATES_ERROR)
  def remove(sourceId: Long, updatedAt: Time) = throw new ShardException(UPDATES_ERROR)

  def negate(sourceId: Long, destinationId: Long, position: Long, updatedAt: Time) = throw new ShardException(UPDATES_ERROR)
  def negate(sourceId: Long, updatedAt: Time) = throw new ShardException(UPDATES_ERROR)

  def withLock[A](sourceId: Long)(f: (Shard, Metadata) => A) = throw new ShardException(LOCKS_ERROR)
  
  def intersect(sourceId: Long, states: Seq[State], destinationIds: Seq[Long]) = throw new ShardException(READS_ERROR)
  def intersectEdges(sourceId: Long, states: Seq[State], destinationIds: Seq[Long]) = throw new ShardException(READS_ERROR)
}