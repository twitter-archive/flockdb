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

import java.util.Random
import java.sql.{BatchUpdateException, ResultSet, SQLException, SQLIntegrityConstraintViolationException}
import scala.collection.mutable
import com.twitter.gizzard.proxy.SqlExceptionWrappingProxy
import com.twitter.gizzard.shards
import com.twitter.ostrich.Stats
import com.twitter.querulous.config.Connection
import com.twitter.querulous.evaluator.{QueryEvaluator, QueryEvaluatorFactory, Transaction}
import com.twitter.querulous.query
import com.twitter.querulous.query.{SqlQueryTimeoutException}
import com.twitter.util.Time
import com.twitter.util.TimeConversions._
import com.mysql.jdbc.exceptions.MySQLTransactionRollbackException
import net.lag.configgy.ConfigMap
import net.lag.logging.Logger
import State._

object QueryClass {
  val Select       = query.QueryClass.Select
  val Execute      = query.QueryClass.Execute
  val SelectModify = query.QueryClass("select_modify")
  val SelectCopy   = query.QueryClass("select_copy")
}

class SqlShardFactory(instantiatingQueryEvaluatorFactory: QueryEvaluatorFactory, materializingQueryEvaluatorFactory: QueryEvaluatorFactory, connection: Connection)
  extends shards.ShardFactory[Shard] {

  val deadlockRetries = 3

  val EDGE_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS %s (
  source_id             %s                       NOT NULL,
  position              BIGINT                   NOT NULL,
  updated_at            INT UNSIGNED             NOT NULL,
  destination_id        %s                       NOT NULL,
  count                 TINYINT UNSIGNED         NOT NULL,
  state                 TINYINT                  NOT NULL,

  PRIMARY KEY (source_id, state, position),

  UNIQUE unique_source_id_destination_id (source_id, destination_id)
) TYPE=INNODB"""

  val METADATA_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS %s (
  source_id             %s                       NOT NULL,
  count                 INT                      NOT NULL,
  state                 TINYINT                  NOT NULL,
  updated_at            INT UNSIGNED             NOT NULL,

  PRIMARY KEY (source_id)
) TYPE=INNODB
"""

  def instantiate(shardInfo: shards.ShardInfo, weight: Int, children: Seq[Shard]) = {
    val queryEvaluator = instantiatingQueryEvaluatorFactory(connection.withHost(shardInfo.hostname))
    new SqlExceptionWrappingProxy(shardInfo.id).apply[Shard](new SqlShard(queryEvaluator, shardInfo, weight, children, deadlockRetries))
  }

  def materialize(shardInfo: shards.ShardInfo) = {
    try {
      val queryEvaluator = materializingQueryEvaluatorFactory(connection.withHost(shardInfo.hostname).withoutDatabase)
      queryEvaluator.execute("CREATE DATABASE IF NOT EXISTS " + connection.database)
      queryEvaluator.execute(EDGE_TABLE_DDL.format(connection.database + "." + shardInfo.tablePrefix + "_edges", shardInfo.sourceType, shardInfo.destinationType))
      queryEvaluator.execute(METADATA_TABLE_DDL.format(connection.database + "." + shardInfo.tablePrefix + "_metadata", shardInfo.sourceType))
    } catch {
      case e: SQLException => throw new shards.ShardException(e.toString)
      case e: SqlQueryTimeoutException => throw new shards.ShardTimeoutException(e.timeout, shardInfo.id, e)
    }
  }
}


class SqlShard(val queryEvaluator: QueryEvaluator, val shardInfo: shards.ShardInfo,
               val weight: Int, val children: Seq[Shard], deadlockRetries: Int) extends Shard {
  val log = Logger.get(getClass.getName)
  private val tablePrefix = shardInfo.tablePrefix
  private val randomGenerator = new Random

  import QueryClass._

  def get(sourceId: Long, destinationId: Long) = {
    queryEvaluator.selectOne("SELECT * FROM " + tablePrefix + "_edges WHERE source_id = ? AND destination_id = ?", sourceId, destinationId) { row =>
      makeEdge(row)
    }
  }

  def getMetadata(sourceId: Long): Option[Metadata] = {
    queryEvaluator.selectOne("SELECT * FROM " + tablePrefix + "_metadata WHERE source_id = ?", sourceId) { row =>
      Metadata(sourceId, State(row.getInt("state")), row.getInt("count"), Time(row.getInt("updated_at").seconds))
    }
  }

  def selectAllMetadata(cursor: Cursor, count: Int) = {
    val metadatas = new mutable.ArrayBuffer[Metadata]
    var nextCursor = Cursor.Start
    var returnedCursor = Cursor.End

    var i = 0
    val query = "SELECT * FROM " + tablePrefix +
      "_metadata WHERE source_id > ? ORDER BY source_id LIMIT ?"
    queryEvaluator.select(SelectCopy, query, cursor.position, count + 1) { row =>
      if (i < count) {
        val sourceId = row.getLong("source_id")
        metadatas += Metadata(sourceId, State(row.getInt("state")), row.getInt("count"),
                              Time(row.getInt("updated_at").seconds))
        nextCursor = Cursor(sourceId)
        i += 1
      } else {
        returnedCursor = nextCursor
      }
    }

    (metadatas, returnedCursor)
  }

  def count(sourceId: Long, states: Seq[State]): Int = {
    queryEvaluator.selectOne("SELECT state, `count` FROM " + tablePrefix + "_metadata WHERE source_id = ?", sourceId) { row =>
      states.foldLeft(0) { (result, state) =>
        result + (if (state == State(row.getInt("state"))) row.getInt("count") else 0)
      }
    } getOrElse {
      populateMetadata(sourceId, Normal)
      count(sourceId, states)
    }
  }

  def counts(sourceIds: Seq[Long], results: mutable.Map[Long, Int]) {
    queryEvaluator.select("SELECT source_id, `count` FROM " + tablePrefix + "_metadata WHERE source_id IN (?)", sourceIds) { row =>
      results(row.getLong("source_id")) = row.getInt("count")
    }
  }

  private def populateMetadata(sourceId: Long, state: State) { populateMetadata(sourceId, state, Time(0.seconds)) }

  private def populateMetadata(sourceId: Long, state: State, updatedAt: Time) {
    try {
      queryEvaluator.execute(
        "INSERT INTO " + tablePrefix + "_metadata (source_id, count, state, updated_at) VALUES (?, ?, ?, ?)",
        sourceId,
        computeCount(sourceId, state),
        state.id,
        updatedAt.inSeconds)
    } catch {
      case e: SQLIntegrityConstraintViolationException =>
    }
  }

  private def computeCount(sourceId: Long, state: State) = {
    queryEvaluator.count("SELECT count(*) FROM " + tablePrefix + "_edges WHERE source_id = ? AND state = ?", sourceId, state.id)
  }

  def selectAll(cursor: (Cursor, Cursor), count: Int): (Seq[Edge], (Cursor, Cursor)) = {
    val edges = new mutable.ArrayBuffer[Edge]
    var nextCursor = (Cursor.Start, Cursor.Start)
    var returnedCursor = (Cursor.End, Cursor.End)

    var i = 0
    val query = "SELECT * FROM " + tablePrefix + "_edges " +
      "USE INDEX (unique_source_id_destination_id) WHERE (source_id = ? AND destination_id > ?) " +
      "OR (source_id > ?) ORDER BY source_id, destination_id LIMIT ?"
    val (cursor1, cursor2) = cursor
    queryEvaluator.select(SelectCopy, query, cursor1.position, cursor2.position, cursor1.position,
                          count + 1) { row =>
      if (i < count) {
        edges += makeEdge(row)
        nextCursor = (Cursor(row.getLong("source_id")), Cursor(row.getLong("destination_id")))
        i += 1
      } else {
        returnedCursor = nextCursor
      }
    }

    (edges, returnedCursor)
  }

  private def existingMetadata(ids: Collection[Long]): Seq[Long] = {
    queryEvaluator.select("SELECT source_id FROM " + tablePrefix + "_metadata WHERE source_id IN (?)", ids.toList) { row =>
      row.getLong("source_id")
    }
  }

  private def existingEdges(edges: Collection[Edge]) = {
    val where = edges.map{edge => "(source_id = " + edge.sourceId + " AND destination_id=" +edge.destinationId+")"}.mkString(" OR ")
    val query = "SELECT source_id, destination_id FROM " + tablePrefix + "_edges WHERE " + where

    val set = new mutable.HashSet[(Long, Long)]
    queryEvaluator.select(query) { row =>
      set += ((row.getLong("source_id"), row.getLong("destination_id")))
    }
    set
  }

  private def statePriority(state: String): String = "-IF(" + state + "=0, 4, " + state + ")"

  private def initializeMetadata(queryEvaluator: QueryEvaluator, sourceIds: Set[Long]): Unit = {
    val newIds = sourceIds -- existingMetadata(sourceIds)
    if (!newIds.isEmpty) {
      val values = newIds.map("(" + _ + ")").mkString(",")
      val query = "INSERT IGNORE INTO " + tablePrefix + "_metadata (source_id) VALUES " + values
      queryEvaluator.execute(query)
    }
  }

  private def initializeEdges(queryEvaluator: QueryEvaluator, edges: Seq[Edge]) = {
    if (!edges.isEmpty) {
      val existing = existingEdges(edges)
      val filtered = edges.filter{ edge => !existing.contains((edge.sourceId, edge.destinationId)) }
      if(!filtered.isEmpty){
        // FIXME: WTF DIY SQL
        val values = filtered.map{ edge => "(" + edge.sourceId + ", " + edge.destinationId + ", 0, 0, "+edge.position+", -1)"}.mkString(",")
        val query = "INSERT IGNORE INTO " + tablePrefix + "_edges (source_id, destination_id, updated_at, count, position, state) VALUES " + values
        queryEvaluator.execute(query)
      }
    }
  }

  private def incr(newColor: String) = {
    "IF(" + newColor + " = edges.state, 0, " +
      "IF(metadata.state = " + newColor + ", 1, IF(metadata.state = edges.state, -1, 0)))"
  }

  private def write(edge: Edge) {
    write(Seq(edge), deadlockRetries)
  }

  private def write(edges: Seq[Edge], tries: Int) {
    def time[A](f: => A) = { val start = Time.now; f; Time.now - start }

    if (!edges.isEmpty) {
      try {
        val totalTime = time {
          log.info("start writing "+ edges.length +" edges")
          queryEvaluator.transaction { transaction =>
            val sourceIdSet = Set(edges.map(_.sourceId): _*)
            val metaInitTime = time {
              initializeMetadata(transaction, sourceIdSet)
            }
            log.info("init metadata ("+ sourceIdSet.size +" rows) elapsed millis: "+ metaInitTime.inMilliseconds)

            val edgesInitTime = time {
              initializeEdges(transaction, edges)
            }
            log.info("init edges ("+ edges.length +" rows) elapsed millis: "+ edgesInitTime.inMilliseconds)

            val query = "UPDATE " + tablePrefix + "_metadata AS metadata, " + tablePrefix + "_edges AS edges " +
              "SET " +
              "    metadata.count = metadata.count + " + incr("?") + "," +
              "    edges.state            = ?, " +
              "    edges.position         = IF(metadata.state = 1, ?, edges.position), " +
              "    edges.updated_at       = ? " +
              "WHERE (edges.updated_at    < ? OR (edges.updated_at = ? AND " +
              "(" + statePriority("edges.state") + " < " + statePriority("?") + ")))" +
              "  AND edges.source_id      = ? " +
              "  AND edges.destination_id = ? " +
              "  AND metadata.source_id   = ? "
            val edgeWriteTimes = edges.map { edge =>
              time {
                transaction.execute(query, edge.state.id, edge.state.id, edge.state.id, edge.position, edge.updatedAt.inSeconds, edge.updatedAt.inSeconds, edge.updatedAt.inSeconds, edge.state.id, edge.state.id, edge.sourceId, edge.destinationId, edge.sourceId)
              }.inMilliseconds
            }

            val edgeWriteAvg = edgeWriteTimes.reduceLeft(_ + _) / edges.length.toFloat

            log.info("average edge write millis: "+ edgeWriteAvg)
          }
        }

        log.info("end writing "+ edges.length +" edges. Elapsed millis: "+ totalTime.inMilliseconds)
      } catch {
        case e: MySQLTransactionRollbackException if (tries > 0) =>
          write(edges, tries - 1)
      }
    }
  }

  def selectByDestinationId(sourceId: Long, states: Seq[State], count: Int, cursor: Cursor) = {
    select("destination_id", "unique_source_id_destination_id", count, cursor,
      "source_id = ? AND state IN (?)",
      List(sourceId, states.map(_.id).toList): _*)
  }

  def selectIncludingArchived(sourceId: Long, count: Int, cursor: Cursor) = {
    select(SelectModify, "destination_id", "unique_source_id_destination_id", count, cursor,
      "source_id = ? AND state != ?",
      sourceId, Removed.id)
  }

  def selectByPosition(sourceId: Long, states: Seq[State], count: Int, cursor: Cursor) = {
    select("position", "PRIMARY", count, cursor,
      "source_id = ? AND state IN (?)",
      List(sourceId, states.map(_.id).toList): _*)
  }

  private def select(cursorName: String, index: String, count: Int,
                     cursor: Cursor, conditions: String, args: Any*): ResultWindow[Long] = {
    select(QueryClass.Select, cursorName, index, count, cursor, conditions, args: _*)
  }

  private def select(queryClass: querulous.query.QueryClass, cursorName: String, index: String, count: Int,
                     cursor: Cursor, conditions: String, args: Any*): ResultWindow[Long] = {
    var edges = new mutable.ArrayBuffer[(Long, Cursor)]
    val order = if (cursor < Cursor.Start) "ASC" else "DESC"
    val inequality = if (order == "DESC") "<" else ">"

    val (continueCursorQuery, args1) = query(cursorName, index, 1, cursor, opposite(order), opposite(inequality), conditions, args)
    val (edgesQuery, args2) = query(cursorName, index, count + 1, cursor, order, inequality, conditions, args)
    val totalQuery = continueCursorQuery + " UNION " + edgesQuery
    queryEvaluator.select(queryClass, totalQuery, args1 ++ args2: _*) { row =>
      edges += (row.getLong("destination_id"), Cursor(row.getLong(cursorName)))
    }

    var page = edges.projection
    if (cursor < Cursor.Start) page = page.reverse
    new ResultWindow(page, count, cursor)
  }

  def selectEdges(sourceId: Long, states: Seq[State], count: Int, cursor: Cursor) = {
    val conditions = "source_id = ? AND state IN (?)"
    val order = if (cursor < Cursor.Start) "ASC" else "DESC"
    val inequality = if (order == "DESC") "<" else ">"
    val args = sourceId :: states.map(_.id).toList
    val (edgesQuery, args1) = query("*", "position", "PRIMARY", count + 1, cursor, order, inequality, conditions, args)
    val (continueCursorQuery, args2) = query("*", "position", "PRIMARY", 1, cursor, opposite(order), opposite(inequality), conditions, args)

    val edges = new mutable.ArrayBuffer[(Edge, Cursor)]
    queryEvaluator.select(continueCursorQuery + " UNION " + edgesQuery, args1 ++ args2: _*) { row =>
      edges += (makeEdge(row), Cursor(row.getLong("position")))
    }

    var page = edges.projection
    if (cursor < Cursor.Start) page = page.reverse
    new ResultWindow(page, count, cursor)
  }

  private def query(cursorName: String, index: String, count: Int, cursor: Cursor, order: String,
                    inequality: String, conditions: String, args: Seq[Any]): (String, Seq[Any]) = {
    val projections = Set("destination_id", cursorName).mkString(", ")
    query(projections, cursorName, index, count, cursor, order, inequality, conditions, args)
  }

  private def query(projections: String, cursorName: String, index: String, count: Int,
                    cursor: Cursor, order: String, inequality: String, conditions: String, args: Seq[Any]): (String, Seq[Any]) = {
    val position = if (cursor == Cursor.Start) Math.MAX_LONG else cursor.magnitude.position

    val query = "(SELECT " + projections +
      " FROM "     + tablePrefix + "_edges USE INDEX (" + index + ")" +
      " WHERE "    + conditions +
      "   AND "    + cursorName + " " + inequality + "?" +
      " ORDER BY " + cursorName + " " + order +
      " LIMIT "    + count + ")"
    (query, args ++ List(position))
  }

  private def opposite(direction: String) = direction match {
    case "ASC" => "DESC"
    case "DESC" => "ASC"
    case "<" => ">="
    case ">" => "<="
  }

  def intersect(sourceId: Long, states: Seq[State], destinationIds: Seq[Long]) = {
    if (destinationIds.size == 0) Nil else {
      queryEvaluator.select("SELECT destination_id FROM " + tablePrefix + "_edges WHERE source_id = ? AND state IN (?) AND destination_id IN (?) ORDER BY destination_id DESC",
        List(sourceId, states.map(_.id).toList, destinationIds): _*) { row =>
        row.getLong("destination_id")
      }
    }
  }

  def intersectEdges(sourceId: Long, states: Seq[State], destinationIds: Seq[Long]) = {
    if (destinationIds.size == 0) Nil else {
      queryEvaluator.select("SELECT * FROM " + tablePrefix + "_edges WHERE source_id = ? AND state IN (?) AND destination_id IN (?) ORDER BY destination_id DESC",
        List(sourceId, states.map(_.id).toList, destinationIds): _*) { row =>
        makeEdge(row)
      }
    }
  }

  def add(sourceId: Long, destinationId: Long, position: Long, updatedAt: Time) = {
    write(new Edge(sourceId, destinationId, position, updatedAt, Normal))
  }

  def add(sourceId: Long, updatedAt: Time) {
    updateMetadata(sourceId, Normal, updatedAt)
  }

  def negate(sourceId: Long, destinationId: Long, position: Long, updatedAt: Time) = {
    write(new Edge(sourceId, destinationId, position, updatedAt, Negative))
  }

  def negate(sourceId: Long, updatedAt: Time) {
    updateMetadata(sourceId, Negative, updatedAt)
  }

  def remove(sourceId: Long, destinationId: Long, position: Long, updatedAt: Time) = {
    write(new Edge(sourceId, destinationId, position, updatedAt, Removed))
  }

  def remove(sourceId: Long, updatedAt: Time) {
    updateMetadata(sourceId, Removed, updatedAt)
  }

  def archive(sourceId: Long, destinationId: Long, position: Long, updatedAt: Time) = {
    write(new Edge(sourceId, destinationId, position, updatedAt, Archived))
  }

  def archive(sourceId: Long, updatedAt: Time) {
    updateMetadata(sourceId, Archived, updatedAt)
  }

  override def equals(other: Any) = {
    other match {
      case other: SqlShard =>
        tablePrefix == other.tablePrefix && queryEvaluator == other.queryEvaluator
      case _ =>
        false
    }
  }

  override def hashCode = {
    (if (tablePrefix == null) 37 else tablePrefix.hashCode * 37) + (if(queryEvaluator == null) 1 else queryEvaluator.hashCode)
  }

  private class MissingMetadataRow extends Exception("Missing Count Row")

  private def insertEdge(transaction: Transaction, metadata: Metadata, edge: Edge): Int = {
    val insertedRows =
      transaction.execute("INSERT INTO " + tablePrefix + "_edges (source_id, position, " +
                          "updated_at, destination_id, state, count) VALUES (?, ?, ?, ?, ?, 0)",
                          edge.sourceId, edge.position, edge.updatedAt.inSeconds,
                          edge.destinationId, edge.state.id)
    if (edge.state == metadata.state) insertedRows else 0
  }

  def bulkUnsafeInsertEdges(edges: Seq[Edge]) {
    bulkUnsafeInsertEdges(queryEvaluator, edges)
  }

  def bulkUnsafeInsertEdges(transaction: QueryEvaluator, edges: Seq[Edge]) = {
    if (edges.size > 0) {
      val query = "INSERT INTO " + tablePrefix + "_edges (source_id, position, updated_at, destination_id, state, count) VALUES (?, ?, ?, ?, ?, 0)"
      transaction.executeBatch(query) { batch =>
        edges.foreach { edge =>
          batch(edge.sourceId, edge.position, edge.updatedAt.inSeconds, edge.destinationId, edge.state.id)
        }
      }
    }
  }

  def bulkUnsafeInsertMetadata(metadatas: Seq[Metadata]) = {
    if (metadatas.length > 0) {
      val query = "INSERT INTO " + tablePrefix + "_metadata (source_id, count, state, updated_at) VALUES (?, ?, ?, ?)"
      queryEvaluator.executeBatch(query) { batch =>
        metadatas.foreach { metadata =>
          batch(metadata.sourceId, metadata.count, metadata.state.id, metadata.updatedAt.inSeconds)
        }
      }
    }
  }

  private def updateEdge(transaction: Transaction, metadata: Metadata, edge: Edge,
                         oldEdge: Edge): Int = {
    if ((oldEdge.updatedAt == edge.updatedAt) && (oldEdge.state max edge.state) != edge.state) return 0

    val updatedRows = if (
      oldEdge.state != Archived &&  // Only update position when coming from removed or negated into normal
      oldEdge.state != Normal &&
      edge.state == Normal
    ) {
      transaction.execute("UPDATE " + tablePrefix + "_edges SET updated_at = ?, " +
                          "position = ?, count = 0, state = ? " +
                          "WHERE source_id = ? AND destination_id = ? AND " +
                          "updated_at <= ?",
                          edge.updatedAt.inSeconds, edge.position, edge.state.id,
                          edge.sourceId, edge.destinationId, edge.updatedAt.inSeconds)
    } else {
      try {
        transaction.execute("UPDATE " + tablePrefix + "_edges SET updated_at = ?, " +
                            "count = 0, state = ? " +
                            "WHERE source_id = ? AND destination_id = ? AND updated_at <= ?",
                            edge.updatedAt.inSeconds, edge.state.id, edge.sourceId,
                            edge.destinationId, edge.updatedAt.inSeconds)
      } catch {
        case e: SQLIntegrityConstraintViolationException =>
          // usually this is a (source_id, state, position) violation. scramble the position more.
          // FIXME: hacky. remove with the new schema.
          transaction.execute("UPDATE " + tablePrefix + "_edges SET updated_at = ?, " +
                              "count = 0, state = ?, position = position + ? " +
                              "WHERE source_id = ? AND destination_id = ? AND updated_at <= ?",
                              edge.updatedAt.inSeconds, edge.state.id,
                              (randomGenerator.nextInt() % 999) + 1, edge.sourceId,
                              edge.destinationId, edge.updatedAt.inSeconds)
      }
    }
    if (edge.state != oldEdge.state &&
        (oldEdge.state == metadata.state || edge.state == metadata.state)) updatedRows else 0
  }

  /**
   * Mysql may throw an exception for a bulk operation that actually partially completed.
   * If that happens, try to pick up the pieces and indicate what happened.
   */
  case class BurstResult(completed: Seq[Edge], failed: Seq[Edge])


  def writeBurst(transaction: Transaction, edges: Seq[Edge]): BurstResult = {
    try {
      bulkUnsafeInsertEdges(transaction, edges)
      BurstResult(edges, Nil)
    } catch {
      case e: BatchUpdateException =>
        val completed = new mutable.ArrayBuffer[Edge]
        val failed = new mutable.ArrayBuffer[Edge]
        e.getUpdateCounts().zip(edges.toArray).foreach { case (errorCode, edge) =>
          if (errorCode < 0) {
            failed += edge
          } else {
            completed += edge
          }
        }
        BurstResult(completed, failed)
    }
  }

  private def updateCount(transaction: QueryEvaluator, sourceId: Long, countDelta: Int) = {
    transaction.execute("UPDATE " + tablePrefix + "_metadata SET count = count + ? " +
                        "WHERE source_id = ?", countDelta, sourceId)
  }

  // returns +1, 0, or -1, depending on how the metadata count should change after this operation.
  // `predictExistence`=true for normal operations, false for copy/migrate.
  private def writeEdgeOld(transaction: Transaction, metadata: Metadata, edge: Edge,
                        predictExistence: Boolean): Int = {
    val countDelta = if (predictExistence) {
      transaction.selectOne(SelectModify,
                            "SELECT * FROM " + tablePrefix + "_edges WHERE source_id = ? " +
                            "and destination_id = ?", edge.sourceId, edge.destinationId) { row =>
        makeEdge(row)
      }.map { oldRow =>
        updateEdge(transaction, metadata, edge, oldRow)
      }.getOrElse {
        insertEdge(transaction, metadata, edge)
      }
    } else {
      try {
        insertEdge(transaction, metadata, edge)
      } catch {
        case e: SQLIntegrityConstraintViolationException =>
          transaction.selectOne(SelectModify,
                                "SELECT * FROM " + tablePrefix + "_edges WHERE source_id = ? " +
                                "and destination_id = ?", edge.sourceId, edge.destinationId) { row =>
            makeEdge(row)
          }.map { oldRow =>
            updateEdge(transaction, metadata, edge, oldRow)
          }.getOrElse(0)
      }
    }
    if (edge.state == metadata.state) countDelta else -countDelta
  }


  def writeCopies(edges: Seq[Edge]) {
    if (!edges.isEmpty) {
      Stats.addTiming("x-copy-burst", edges.size)

      var sourceIdsSet = Set[Long]()
      edges.foreach { edge => sourceIdsSet += edge.sourceId }
      val sourceIds = sourceIdsSet.toSeq

      atomically(sourceIds) { (transaction, metadataById) =>
        val result = writeBurst(transaction, edges)
        if (result.completed.size > 0) {
          var currentSourceId = -1L
          var countDeltas = new Array[Int](4)
          result.completed.foreach { edge =>
            if (edge.sourceId != currentSourceId) {
              if (currentSourceId != -1)
                updateCount(transaction, currentSourceId, countDeltas(metadataById(currentSourceId).state.id))
              currentSourceId = edge.sourceId
              countDeltas = new Array[Int](4)
            }
            countDeltas(edge.state.id) += 1
          }
          updateCount(transaction, currentSourceId,
                      countDeltas(metadataById(currentSourceId).state.id))
        }

        if (result.failed.size > 0) {
          Stats.incr("x-copy-fallback")
          var currentSourceId = -1L
          var countDelta = 0
          result.failed.foreach { edge =>
            if (edge.sourceId != currentSourceId) {
              if (currentSourceId != -1)
                updateCount(transaction, currentSourceId, countDelta)
              currentSourceId = edge.sourceId
              countDelta = 0
            }
            countDelta += writeEdgeOld(transaction, metadataById(edge.sourceId), edge, false)
          }
          updateCount(transaction, currentSourceId, countDelta)
        }
      }
    }
  }

  def withLock[A](sourceId: Long)(f: (Shard, Metadata) => A) = {
    atomically(sourceId) { (transaction, metadata) =>
      f(new SqlShard(transaction, shardInfo, weight, children, deadlockRetries), metadata)
    }
  }

  private def atomically[A](sourceId: Long)(f: (Transaction, Metadata) => A): A = {
    try {
      queryEvaluator.transaction { transaction =>
        transaction.selectOne(SelectModify,
                              "SELECT * FROM " + tablePrefix + "_metadata WHERE source_id = ? FOR UPDATE", sourceId) { row =>
          f(transaction, Metadata(sourceId, State(row.getInt("state")), row.getInt("count"), Time(row.getInt("updated_at").seconds)))
        } getOrElse(throw new MissingMetadataRow)
      }
    } catch {
      case e: MissingMetadataRow =>
        populateMetadata(sourceId, Normal)
        atomically(sourceId)(f)
    }
  }

  private def atomically[A](sourceIds: Seq[Long])(f: (Transaction, collection.Map[Long, Metadata]) => A): A = {
    try {
      val metadataMap = mutable.Map[Long, Metadata]()
      queryEvaluator.transaction { transaction =>
        transaction.select(SelectModify,
                           "SELECT * FROM " + tablePrefix + "_metadata WHERE source_id in (?) FOR UPDATE", sourceIds) { row =>
          metadataMap.put(row.getLong("source_id"), Metadata(row.getLong("source_id"), State(row.getInt("state")), row.getInt("count"), Time(row.getInt("updated_at").seconds)))
        }
        if (metadataMap.size < sourceIds.length)
          throw new MissingMetadataRow
        f(transaction, metadataMap)
      }
    } catch {
      case e: MissingMetadataRow =>
        sourceIds.foreach { sourceId => populateMetadata(sourceId, Normal) }
        atomically(sourceIds)(f)
    }
  }

  def writeMetadata(metadata: Metadata) {
    try {
      queryEvaluator.execute("INSERT INTO " + tablePrefix + "_metadata (source_id, count, state, " +
                             "updated_at) VALUES (?, ?, ?, ?)",
                             metadata.sourceId, 0, metadata.state.id, metadata.updatedAt.inSeconds)
    } catch {
      case e: SQLIntegrityConstraintViolationException =>
        atomically(metadata.sourceId) { (transaction, oldMetadata) =>
          transaction.execute("UPDATE " + tablePrefix + "_metadata SET state = ?, updated_at = ? " +
                              "WHERE source_id = ? AND updated_at <= ?",
                              metadata.state.id, metadata.updatedAt.inSeconds, metadata.sourceId,
                              metadata.updatedAt.inSeconds)
        }
    }
  }

  def writeMetadata(metadatas: Seq[Metadata])  = {
    if (!metadatas.isEmpty) {
      try {
        val query = "INSERT INTO " + tablePrefix + "_metadata (source_id, count, state, updated_at) VALUES (?, 0, ?, ?)"
        queryEvaluator.executeBatch(query) { batch =>
          metadatas.foreach { metadata =>
            batch(metadata.sourceId, metadata.state.id, metadata.updatedAt.inSeconds)
          }
        }
      } catch {
        case e: BatchUpdateException =>
          e.getUpdateCounts().zip(metadatas.toArray).foreach { case (errorCode, metadata) =>
            if (errorCode < 0)
              writeMetadata(metadata)
          }
      }
    }
  }

  def updateMetadata(metadata: Metadata): Unit = updateMetadata(metadata.sourceId, metadata.state, metadata.updatedAt)

  // FIXME: computeCount could be really expensive. :(
  def updateMetadata(sourceId: Long, state: State, updatedAt: Time) {
    atomically(sourceId) { (transaction, metadata) =>
      if ((updatedAt != metadata.updatedAt) || ((metadata.state max state) == state)) {
        transaction.execute("UPDATE " + tablePrefix + "_metadata SET state = ?, updated_at = ?, count = ? WHERE source_id = ? AND updated_at <= ?",
          state.id, updatedAt.inSeconds, computeCount(sourceId, state), sourceId, updatedAt.inSeconds)
      }
    }
  }

  private def makeEdge(sourceId: Long, destinationId: Long, position: Long, updatedAt: Time, stateId: Int): Edge = {
    new Edge(sourceId, destinationId, position, updatedAt, State(stateId))
  }

  private def makeEdge(row: ResultSet): Edge = {
    makeEdge(row.getLong("source_id"), row.getLong("destination_id"), row.getLong("position"), Time(row.getInt("updated_at").seconds), row.getInt("state"))
  }
}
