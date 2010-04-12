package com.twitter.flockdb.jobs

import com.twitter.gizzard.jobs.BoundJobParser
import com.twitter.gizzard.scheduler.JobScheduler
import com.twitter.gizzard.nameserver.NameServer
import com.twitter.results
import com.twitter.ostrich.Stats
import com.twitter.xrayspecs.TimeConversions._
import net.lag.logging.Logger
import shards.Shard


object Copy {
  type Cursor = (results.Cursor, results.Cursor)

  val START = (results.Cursor.Start, results.Cursor.Start)
  val END = (results.Cursor.End, results.Cursor.End)
  val COUNT = 10000
}

object CopyFactory extends gizzard.jobs.CopyFactory[Shard] {
  def apply(sourceShardId: Int, destinationShardId: Int) = new MetadataCopy(sourceShardId, destinationShardId, MetadataCopy.START)
}

class Copy(sourceShardId: Int, destinationShardId: Int, cursor: Copy.Cursor, count: Int) extends gizzard.jobs.Copy[Shard](sourceShardId, destinationShardId, count) {
  def this(sourceShardId: Int, destinationShardId: Int, cursor: Copy.Cursor) = this(sourceShardId, destinationShardId, cursor, Copy.COUNT)
  def this(attributes: Map[String, AnyVal]) = {
    this(
      attributes("source_shard_id").toInt,
      attributes("destination_shard_id").toInt,
      (results.Cursor(attributes("cursor1").toInt), results.Cursor(attributes("cursor2").toInt)),
      attributes("count").toInt)
  }

  def copyPage(sourceShard: Shard, destinationShard: Shard, count: Int) = {
    val (items, newCursor) = sourceShard.selectAll(cursor, count)
    destinationShard.writeCopies(items)
    Stats.incr("edges-copy", items.size)
    if (newCursor == Copy.END)
      None
    else
      Some(new Copy(sourceShardId, destinationShardId, newCursor, count))
  }

  def serialize = Map("cursor1" -> cursor._1.position, "cursor2" -> cursor._2.position)
}

object MetadataCopy {
  type Cursor = results.Cursor
  val START = results.Cursor.Start
  val END = results.Cursor.End
}

class MetadataCopy(sourceShardId: Int, destinationShardId: Int, cursor: MetadataCopy.Cursor, count: Int) extends gizzard.jobs.Copy[Shard](sourceShardId, destinationShardId, count) {
  def this(sourceShardId: Int, destinationShardId: Int, cursor: MetadataCopy.Cursor) = this(sourceShardId, destinationShardId, cursor, Copy.COUNT)
  def this(attributes: Map[String, AnyVal]) = {
    this(
      attributes("source_shard_id").toInt,
      attributes("destination_shard_id").toInt,
      results.Cursor(attributes("cursor").toInt),
      attributes("count").toInt)
  }

  def copyPage(sourceShard: Shard, destinationShard: Shard, count: Int) = {
    val (items, newCursor) = sourceShard.selectAllMetadata(cursor, count)
    items.foreach { destinationShard.writeMetadata(_) }
    Stats.incr("edges-copy", items.size)
    if (newCursor == MetadataCopy.END)
      Some(new Copy(sourceShardId, destinationShardId, Copy.START))
    else
      Some(new MetadataCopy(sourceShardId, destinationShardId, newCursor, count))
  }

  def serialize = Map("cursor" -> cursor.position)
}
