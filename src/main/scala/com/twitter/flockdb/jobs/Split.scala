// /*
//  * Copyright 2010 Twitter, Inc.
//  *
//  * Licensed under the Apache License, Version 2.0 (the "License"); you may
//  * not use this file except in compliance with the License. You may obtain
//  * a copy of the License at
//  *
//  *     http://www.apache.org/licenses/LICENSE-2.0
//  *
//  * Unless required by applicable law or agreed to in writing, software
//  * distributed under the License is distributed on an "AS IS" BASIS,
//  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  * See the License for the specific language governing permissions and
//  * limitations under the License.
//  */
// 
// package com.twitter.flockdb.jobs
// 
// import com.twitter.gizzard.scheduler._
// import com.twitter.gizzard.shards.ShardId
// import com.twitter.gizzard.nameserver.NameServer
// import com.twitter.ostrich.Stats
// import com.twitter.util.TimeConversions._
// import conversions.Numeric._
// import shards.Shard
// 
// 
// object Split {
//   type SplitCursor = (Cursor, Cursor)
// 
//   val START = (Cursor.Start, Cursor.Start)
//   val END = (Cursor.End, Cursor.End)
//   val COUNT = 10000
// }
// 
// case class Destination(shardId: ShardId, baseId: Long)
// 
// trait DestinationParser {
//   def parseDestinations(attributes: Map[String, Any], baseDestinationShard: ShardId) = {
//     destinations = new ListBuffer[Destination]
//     destinations += Destination(base, 0)
//     var i = 0
//     while(attributes.contains("destination_" + i + "_hostname")) {
//       val prefix = "destination_" + i
//       val baseId = attributes(prefix + "_base_id").asInstanceOf[AnyVal].toLong
//       val shardId = ShardId(attributes(prefix + "_shard_hostname").toString, attributes(prefix + "_shard_table_prefix").toString)
//       destinations += Destination(baseId, shardId)
//       i += 1
//     }
//     
//     destinations.toList
//   }
// }
// 
// class SplitParser(nameServer: NameServer[Shard], scheduler: JobScheduler[JsonJob])
//       extends CopyJobParser[Shard] with DestinationParser {
//   def deserialize(attributes: Map[String, Any], sourceId: ShardId, destinationId: ShardId, count: Int) = {
//     val cursor = (Cursor(attributes("cursor1").asInstanceOf[AnyVal].toLong),
//                   Cursor(attributes("cursor2").asInstanceOf[AnyVal].toLong))
//     
//     val destinations = parseDestinations(attributes, destinationId)            
//     new Split(sourceId, destinations, cursor, count, nameServer, scheduler)
//   }
// }
// 
// class Split(sourceShardId: ShardId, destinations: List[Destination], cursor: Copy.CopyCursor,
//            count: Int, nameServer: NameServer[Shard], scheduler: JobScheduler[JsonJob])
//       extends CopyJob[Shard](sourceShardId, destinations(0).shardId, count, nameServer, scheduler) {
//         
//   def copyPage(sourceShard: Shard, destinationShard: Shard, count: Int) = {
//     val (items, newCursor) = sourceShard.selectAll(cursor, count)
//     destinationShard.writeCopies(items)
//     Stats.incr("edges-copy", items.size)
//     if (newCursor == Copy.END) {
//       None
//     } else {
//       Some(new Copy(sourceShardId, destinationShardId, newCursor, count, nameServer, scheduler))
//     }
//   }
// 
//   def serialize = Map("cursor1" -> cursor._1.position, "cursor2" -> cursor._2.position)
// }
// 
// object MetadataCopy {
//   type CopyCursor = Cursor
//   val START = Cursor.Start
//   val END = Cursor.End
// }
// 
// class MetadataCopyParser(nameServer: NameServer[Shard], scheduler: JobScheduler[JsonJob])
//       extends CopyJobParser[Shard] {
//   def deserialize(attributes: Map[String, Any], sourceId: ShardId, destinationId: ShardId, count: Int) = {
//     val cursor = Cursor(attributes("cursor").asInstanceOf[AnyVal].toLong)
//     new MetadataCopy(sourceId, destinationId, cursor, count, nameServer, scheduler)
//   }
// }
// 
// class MetadataCopy(sourceShardId: ShardId, destinationShardId: ShardId, cursor: MetadataCopy.CopyCursor,
//                    count: Int, nameServer: NameServer[Shard], scheduler: JobScheduler[JsonJob])
//       extends CopyJob[Shard](sourceShardId, destinationShardId, count, nameServer, scheduler) {
//   def copyPage(sourceShard: Shard, destinationShard: Shard, count: Int) = {
//     val (items, newCursor) = sourceShard.selectAllMetadata(cursor, count)
//     items.foreach { destinationShard.writeMetadata(_) }
//     Stats.incr("edges-copy", items.size)
//     if (newCursor == MetadataCopy.END)
//       Some(new Copy(sourceShardId, destinationShardId, Copy.START, Copy.COUNT, nameServer, scheduler))
//     else
//       Some(new MetadataCopy(sourceShardId, destinationShardId, newCursor, count, nameServer, scheduler))
//   }
// 
//   def serialize = Map("cursor" -> cursor.position)
// }
// 
// 
// // 
// // /*
// //  * Copyright 2010 Twitter, Inc.
// //  *
// //  * Licensed under the Apache License, Version 2.0 (the "License"); you may
// //  * not use this file except in compliance with the License. You may obtain
// //  * a copy of the License at
// //  *
// //  *     http://www.apache.org/licenses/LICENSE-2.0
// //  *
// //  * Unless required by applicable law or agreed to in writing, software
// //  * distributed under the License is distributed on an "AS IS" BASIS,
// //  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// //  * See the License for the specific language governing permissions and
// //  * limitations under the License.
// //  */
// // 
// // package com.twitter.flockdb.jobs
// // 
// // import java.util.TreeMap
// // import collection.mutable.ListBuffer
// // import com.twitter.gizzard.scheduler._
// // import com.twitter.gizzard.shards.ShardId
// // import com.twitter.gizzard.nameserver.{NameServer}
// // import com.twitter.ostrich.Stats
// // import com.twitter.util.TimeConversions._
// // import conversions.Numeric._
// // import shards.Shard
// // 
// // object Split {
// //   type SplitCursor = (Cursor, Cursor)
// // 
// //   val START = (Cursor.Start, Cursor.Start)
// //   val END = (Cursor.End, Cursor.End)
// //   val COUNT = 10000
// // }
// // 
// // 
// // case class IdForwarding(baseId: Long, shardId: ShardId)
// // case class Forwarding(shard: Shard, baseId: Long)
// // 
// // class SplitFactory(nameServer: NameServer[Shard], scheduler: JobScheduler[JsonJob]) {
// //   def apply(sourceShardId: ShardId, destinations: List[IdForwarding]) =
// //     new MetadataSplit(sourceShardId, destinations, MetadataCopy.START, Copy.COUNT,
// //                      nameServer, scheduler)
// // }
// // 
// // class SplitParser(nameServer: NameServer[Shard], scheduler: JobScheduler[JsonJob])
// //       extends JsonJobParser {
// //   def apply(attributes: Map[String, Any]): JsonJob = {
// //     val sourceId = ShardId(attributes("source_shard_hostname").toString, attributes("source_shard_table_prefix").toString)
// //     val count = attributes("count").asInstanceOf[{def toInt: Int}].toInt
// //     
// //     var i = 0
// //     val destinations = new ListBuffer[IdForwarding]
// //     while(attributes.contains("destination_" + i + "_hostname")) {
// //       val prefix = "destination_" + i
// //       val baseId = attributes(prefix + "_base_id").asInstanceOf[{def toInt: Int}].toInt
// //       val shardId = ShardId(attributes(prefix + "_shard_hostname").toString, attributes(prefix + "_shard_table_prefix").toString)
// //       destinations += IdForwarding(baseId, shardId)
// //       i += 1
// //     }
// // 
// //     val cursor = (Cursor(attributes("cursor1").asInstanceOf[AnyVal].toLong),
// //                   Cursor(attributes("cursor2").asInstanceOf[AnyVal].toLong))
// //     new Split(sourceId, destinations.toList, cursor, count, nameServer, scheduler)
// //   }
// // }
// // 
// // class Split(sourceShardId: ShardId, destinations: List[IdForwarding], cursor: Split.SplitCursor,
// //            count: Int, nameServer: NameServer[Shard], scheduler: JobScheduler[JsonJob])
// //       extends JsonJob {
// //   def copyPage(sourceShard: Shard, destinationShards: List[Forwarding], count: Int) = {
// //     val (items, newCursor) = sourceShard.selectAll(cursor, count)
// //     
// //     val byBaseIds = new TreeMap[Long, (Shard, ListBuffer[Edge])]()
// //     destinationShards.foreach { d => 
// //       byBaseIds.put(d.baseId, ((d.shard, new ListBuffer[Edge])))
// //     }
// //     
// //     items.foreach { item => 
// //       byBaseIds.floorEntry(nameServer.mappingFunction(item.sourceId)).getValue._2 += item
// //     }
// //     
// //     val iter = byBaseIds.values.iterator
// //     while(iter.hasNext) {
// //       val (dest, list) = iter.next
// //       dest.writeCopies(list)
// //       Stats.incr("edges-split", list.size)
// //     }
// //     
// //     if (newCursor == Split.END) {
// //       None
// //     } else {
// //       Some(new Split(sourceShardId, destinations, newCursor, count, nameServer, scheduler))
// //     }
// //   }
// // 
// //   def serialize = Map("cursor1" -> cursor._1.position, "cursor2" -> cursor._2.position)
// // }
// // 
// // object MetadataSplit {
// //   type SplitCursor = Cursor
// //   val START = Cursor.Start
// //   val END = Cursor.End
// // }
// // 
// // class MetadataSplitParser(nameServer: NameServer[Shard], scheduler: JobScheduler[JsonJob])
// //       extends JsonJobParser {
// //   def deserialize(attributes: Map[String, Any], sourceId: ShardId, destinations: List[IdForwarding], count: Int) = {
// //     val cursor = Cursor(attributes("cursor").asInstanceOf[AnyVal].toLong)
// //     new MetadataSplit(sourceId, destinations, cursor, count, nameServer, scheduler)
// //   }
// // }
// // 
// // class MetadataSplit(sourceShardId: ShardId, destinations: List[IdForwarding], cursor: MetadataSplit.SplitCursor,
// //                    count: Int, nameServer: NameServer[Shard], scheduler: JobScheduler[JsonJob])
// //       extends Job {
// //   def copyPage(sourceShard: Shard, destinationShards: List[Forwarding], count: Int) = {
// //     val (items, newCursor) = sourceShard.selectAllMetadata(cursor, count)
// //     
// //     val byBaseIds = new TreeMap[Long, Shard]()
// //     destinationShards.foreach { d => byBaseIds.put(d.baseId, d.shard) }
// //     
// //     items.foreach { item => 
// //       val shard = byBaseIds.floorEntry(nameServer.mappingFunction(item.sourceId)).getValue
// //       shard.writeMetadata(item)
// //     }
// //     
// //     Stats.incr("edges-metadata-split", items.size)
// //     if (newCursor == MetadataSplit.END)
// //       Some(new Split(sourceShardId, destinations, Split.START, Split.COUNT, nameServer, scheduler))
// //     else
// //       Some(new MetadataSplit(sourceShardId, destinations, newCursor, count, nameServer, scheduler))
// //   }
// // 
// //   def serialize = Map("cursor" -> cursor.position)
// // }
// // 