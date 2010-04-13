package com.twitter.flockdb.shards

import scala.collection.mutable
import com.twitter.results.{Cursor, ResultWindow}
import com.twitter.gizzard.shards
import com.twitter.xrayspecs.Time
import com.twitter.xrayspecs.TimeConversions._


case class Metadata(sourceId: Long, state: State, count: Int, updatedAt: Time)

trait Shard extends shards.Shard {
  @throws(classOf[shards.ShardException]) def get(sourceId: Long, destinationId: Long): Option[Edge]
  @throws(classOf[shards.ShardException]) def getMetadata(sourceId: Long): Option[Metadata]
  @throws(classOf[shards.ShardException]) def withLock[A](sourceId: Long)(f: (Shard, Metadata) => A): A
  @throws(classOf[shards.ShardException]) def count(sourceId: Long, states: Seq[State]): Int
  @throws(classOf[shards.ShardException]) def counts(sourceIds: Seq[Long], results: mutable.Map[Long, Int])

  @throws(classOf[shards.ShardException]) def selectAll(cursor: (Cursor, Cursor), count: Int): (Seq[Edge], (Cursor, Cursor))
  @throws(classOf[shards.ShardException]) def selectAllMetadata(cursor: Cursor, count: Int): (Seq[Metadata], Cursor)
  @throws(classOf[shards.ShardException]) def selectIncludingArchived(sourceId: Long, count: Int, cursor: Cursor): ResultWindow[Long]
  @throws(classOf[shards.ShardException]) def selectByDestinationId(sourceId: Long, states: Seq[State], count: Int, cursor: Cursor): ResultWindow[Long]
  @throws(classOf[shards.ShardException]) def selectByPosition(sourceId: Long, states: Seq[State], count: Int, cursor: Cursor): ResultWindow[Long]
  @throws(classOf[shards.ShardException]) def selectEdges(sourceId: Long, states: Seq[State], count: Int, cursor: Cursor): ResultWindow[Edge]

  @throws(classOf[shards.ShardException]) def writeCopies(edge: Seq[Edge])
  @throws(classOf[shards.ShardException]) def updateMetadata(metadata: Metadata)
  @throws(classOf[shards.ShardException]) def writeMetadata(metadata: Metadata)

  @throws(classOf[shards.ShardException]) def archive(sourceId: Long, destinationId: Long, position: Long, updatedAt: Time)
  @throws(classOf[shards.ShardException]) def archive(sourceId: Long, updatedAt: Time)

  @throws(classOf[shards.ShardException]) def remove(sourceId: Long, destinationId: Long, position: Long, updatedAt: Time)
  @throws(classOf[shards.ShardException]) def remove(sourceId: Long, updatedAt: Time)

  @throws(classOf[shards.ShardException]) def add(sourceId: Long, destinationId: Long, position: Long, updatedAt: Time)
  @throws(classOf[shards.ShardException]) def add(sourceId: Long, updatedAt: Time)

  @throws(classOf[shards.ShardException]) def negate(sourceId: Long, destinationId: Long, position: Long, updatedAt: Time)
  @throws(classOf[shards.ShardException]) def negate(sourceId: Long, updatedAt: Time)

  @throws(classOf[shards.ShardException]) def intersect(sourceId: Long, states: Seq[State], destinationIds: Seq[Long]): Seq[Long]
  @throws(classOf[shards.ShardException]) def intersectEdges(sourceId: Long, states: Seq[State], destinationIds: Seq[Long]): Seq[Edge]
}
