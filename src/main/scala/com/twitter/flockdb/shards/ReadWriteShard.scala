package com.twitter.flockdb.shards

import scala.collection.mutable
import com.twitter.results.Cursor
import com.twitter.gizzard.shards
import com.twitter.service.flock.State
import com.twitter.xrayspecs.Time
import com.twitter.xrayspecs.TimeConversions._


trait ReadWriteShard extends Shard with shards.ReadWriteShard[Shard] {
  def selectIncludingArchived(sourceId: Long, count: Int, cursor: Cursor)                            = readOperation(_.selectIncludingArchived(sourceId, count, cursor))
  def intersect(sourceId: Long, states: Seq[State], destinationIds: Seq[Long])                       = readOperation(_.intersect(sourceId, states, destinationIds))
  def intersectEdges(sourceId: Long, states: Seq[State], destinationIds: Seq[Long])                  = readOperation(_.intersectEdges(sourceId, states, destinationIds))
  def getMetadata(sourceId: Long)                                                                    = readOperation(_.getMetadata(sourceId))
  def selectByDestinationId(sourceId: Long, states: Seq[State], count: Int, cursor: Cursor)          = readOperation(_.selectByDestinationId(sourceId, states, count, cursor))
  def selectByPosition(sourceId: Long, states: Seq[State], count: Int, cursor: Cursor)               = readOperation(_.selectByPosition(sourceId, states, count, cursor))
  def selectEdges(sourceId: Long, states: Seq[State], count: Int, cursor: Cursor)                    = readOperation(_.selectEdges(sourceId, states, count, cursor))
  def selectAll(cursor: (Cursor, Cursor), count: Int)                                                = readOperation(_.selectAll(cursor, count))
  def selectAllMetadata(cursor: Cursor, count: Int)                                                  = readOperation(_.selectAllMetadata(cursor, count))
  def get(sourceId: Long, destinationId: Long)                                                       = readOperation(_.get(sourceId, destinationId))
  def count(sourceId: Long, states: Seq[State])                                                      = readOperation(_.count(sourceId, states))
  def counts(sourceIds: Seq[Long], results: mutable.Map[Long, Int])                                  = readOperation(_.counts(sourceIds, results))

  def writeCopies(edges: Seq[Edge])                                                                  = writeOperation(_.writeCopies(edges))
  def writeMetadata(metadata: Metadata)                                                              = writeOperation(_.writeMetadata(metadata))
  def updateMetadata(metadata: Metadata)                                                             = writeOperation(_.updateMetadata(metadata))
  def remove(sourceId: Long, updatedAt: Time)                                                        = writeOperation(_.remove(sourceId, updatedAt))
  def remove(sourceId: Long, destinationId: Long, position: Long, updatedAt: Time)                   = writeOperation(_.remove(sourceId, destinationId, position, updatedAt))
  def add(sourceId: Long, updatedAt: Time)                                                           = writeOperation(_.add(sourceId, updatedAt))
  def add(sourceId: Long, destinationId: Long, position: Long, updatedAt: Time)                      = writeOperation(_.add(sourceId, destinationId, position, updatedAt))
  def negate(sourceId: Long, updatedAt: Time)                                                        = writeOperation(_.negate(sourceId, updatedAt))
  def negate(sourceId: Long, destinationId: Long, position: Long, updatedAt: Time)                   = writeOperation(_.negate(sourceId, destinationId, position, updatedAt))
  def archive(sourceId: Long, destinationId: Long, position: Long, updatedAt: Time)                  = writeOperation(_.archive(sourceId, destinationId, position, updatedAt))
  def archive(sourceId: Long, updatedAt: Time)                                                       = writeOperation(_.archive(sourceId, updatedAt))

  def withLock[A](sourceId: Long)(f: (Shard, Metadata) => A)                                         = writeOperation(_.withLock(sourceId)(f))
}
