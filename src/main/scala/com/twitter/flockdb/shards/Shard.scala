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
package shards

import com.twitter.util.{Future, Time}

trait Shard {
  def get(sourceId: Long, destinationId: Long): Future[Option[Edge]]
  def getMetadata(sourceId: Long): Future[Option[Metadata]]
  def getMetadataForWrite(sourceId: Long): Future[Option[Metadata]]

  def count(sourceId: Long, states: Seq[State]): Future[Int]

  def selectAll(cursor: (Cursor, Cursor), count: Int): Future[(Seq[Edge], (Cursor, Cursor))]
  def selectAllMetadata(cursor: Cursor, count: Int): Future[(Seq[Metadata], Cursor)]
  def selectIncludingArchived(sourceId: Long, count: Int, cursor: Cursor): Future[ResultWindow[Long]]
  def selectByDestinationId(sourceId: Long, states: Seq[State], count: Int, cursor: Cursor): Future[ResultWindow[Long]]
  def selectByPosition(sourceId: Long, states: Seq[State], count: Int, cursor: Cursor): Future[ResultWindow[Long]]
  def selectEdges(sourceId: Long, states: Seq[State], count: Int, cursor: Cursor): Future[ResultWindow[Edge]]

  def writeCopies(edge: Seq[Edge]): Future[Unit]
  def updateMetadata(metadata: Metadata): Future[Unit]
  def writeMetadata(metadata: Metadata): Future[Unit]
  def writeMetadata(metadata: Seq[Metadata]): Future[Unit]

  def bulkUnsafeInsertEdges(edge: Seq[Edge]): Future[Unit]
  def bulkUnsafeInsertMetadata(edge: Seq[Metadata]): Future[Unit]

  def archive(sourceId: Long, destinationId: Long, position: Long, updatedAt: Time): Future[Unit]
  def archive(sourceId: Long, updatedAt: Time): Future[Unit]

  def remove(sourceId: Long, destinationId: Long, position: Long, updatedAt: Time): Future[Unit]
  def remove(sourceId: Long, updatedAt: Time): Future[Unit]

  def add(sourceId: Long, destinationId: Long, position: Long, updatedAt: Time): Future[Unit]
  def add(sourceId: Long, updatedAt: Time): Future[Unit]

  def negate(sourceId: Long, destinationId: Long, position: Long, updatedAt: Time): Future[Unit]
  def negate(sourceId: Long, updatedAt: Time): Future[Unit]

  def intersect(sourceId: Long, states: Seq[State], destinationIds: Seq[Long]): Future[Seq[Long]]
  def intersectEdges(sourceId: Long, states: Seq[State], destinationIds: Seq[Long]): Future[Seq[Edge]]
}
