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

package com.twitter.flockdb.unit

import scala.io.Source
import java.io.File

import org.specs.Specification

import com.twitter.gizzard.shards.ShardInfo
import com.twitter.elephantbird.util.TypeRef
import com.twitter.elephantbird.mapreduce.output.LzoThriftB64LineRecordWriter
import com.twitter.elephantbird.mapreduce.io.ThriftB64LineWritable
import com.twitter.flockdb.conversions.{Edge, Metadata}
import com.twitter.flockdb.shards.HdfsBackupShard
import com.twitter.flockdb.shards.Metadata
import com.twitter.xrayspecs.Time

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.util.LineReader
import org.apache.thrift.TDeserializer;
import org.apache.commons.codec.binary.Base64;

import State._

object HdfsBackupShardSpec extends ConfiguredSpecification {  
  "Edge HdfsBackupShard" should {
    val FILE_SYSTEM = "file://"
    val TEST_REL_PATH = "tmp/flock_backup"

    val conf = new JobConf
    conf.set("fs.default.name", FILE_SYSTEM + "/")
    val fs = FileSystem.getLocal(conf)
    
    val deserializer = new TDeserializer
    val base64 = new Base64
    
    doAfter {
      fs.delete(new org.apache.hadoop.fs.Path(FILE_SYSTEM + "/" + TEST_REL_PATH))
    }
    
    "writeCopies" in {
      "when there are some edges" >> {
        val edges = List(
          new Edge(1L, 2L, 1, Time.now, 1, Normal),
          new Edge(2L, 1L, 2, Time.now, 1, Removed)
        )
        writeEdges(TEST_REL_PATH, FILE_SYSTEM, edges)
        val checkEdges = readEdges("/" + TEST_REL_PATH + "/edges/data", deserializer, base64)
        edges mustEqual checkEdges
        new File("/" + TEST_REL_PATH + "/edges/_job_finished").exists() mustEqual true
      }
      
      "when there are no edges" >> {
        val edges = List()
        writeEdges(TEST_REL_PATH, FILE_SYSTEM, edges)        
        val checkEdges = readEdges("/" + TEST_REL_PATH + "/edges/data", deserializer, base64)
        edges mustEqual checkEdges
        new File("/" + TEST_REL_PATH + "/edges/_job_finished").exists() mustEqual true
      }
    }
    
    "writeMetadata" in {
      "when there is metadata" >> {
        val metadatas = List(
          new Metadata(1L, Normal, 2, Time.now),
          new Metadata(2L, Removed, 2, Time.now)
        )
        writeMetadatas(TEST_REL_PATH, FILE_SYSTEM, metadatas)
        val checkMetadatas = readMetadatas("/" + TEST_REL_PATH + "/metadata/data", deserializer, base64)
        metadata mustEqual checkMetadatas
        new File("/" + TEST_REL_PATH + "/metadata/_job_finished").exists() mustEqual true
      }
    }
  }
    
  def writeEdges(tableName: String, hostname: String, edges: Seq[Edge]) = {
    val shardInfo = new ShardInfo("com.twitter.flockdb.HdfsBackupShard", tableName, hostname)
    val shard = new HdfsBackupShard(shardInfo, 0, List())
    shard.startEdgeCopy()
    shard.writeCopies(edges)
    shard.finishEdgeCopy()
  }
  
  // Assumes local file system
  def readEdges(path: String, deserializer: TDeserializer, base64: Base64): Seq[Edge] = {
    val lines = Source.fromFile(path).getLines.toList
    lines map { line => 
      val bytes = base64.decode(line.toString.getBytes("UTF-8"))
      val thriftEdge = new thrift.Edge
      deserializer.deserialize(thriftEdge, bytes)
      new Edge.RichThriftEdge(thriftEdge).fromThrift
    }
  }
  
  def writeMetadatas(tableName: String, hostname: String, metadatas: Seq[Metadata]) = {
    val shardInfo = new ShardInfo("com.twitter.flockdb.shards.HdfsBackupShard", tableName, hostname)
    val shard = new HdfsBackupShard(shardInfo, 0, List())
    shard.startMetadataCopy()
    metadatas foreach { metadata => shard.writeMetadata(metadata) }
    shard.finishMetadataCopy()
  }
  
  // Assumes local file system
  def readMetadatas(path: String, deserializer: TDeserializer, base64: Base64): Seq[Metadata] = {
    val lines = Source.fromFile(path).getLines.toList
    lines map { line =>
      val bytes = base64.decode(line.toString.getBytes("UTF-8"))
      val thriftMetadata = new thrift.Metadata
      deserializer.deserialize(thriftMetadata, bytes)
      new com.twitter.flockdb.conversions.Metadata.RichThriftMetadata(thriftMetadata).fromThrift
    }
  }
}
