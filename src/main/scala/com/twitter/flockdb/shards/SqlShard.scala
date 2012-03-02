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

import java.util.Random
import java.sql.{BatchUpdateException, ResultSet, SQLException, SQLIntegrityConstraintViolationException}
import scala.collection.mutable
import com.twitter.gizzard.proxy.SqlExceptionWrappingProxyFactory
import com.twitter.gizzard.Stats
import com.twitter.gizzard.shards._
import com.twitter.querulous.config.Connection
import com.twitter.querulous.async.{AsyncQueryEvaluator, AsyncQueryEvaluatorFactory}
import com.twitter.querulous.evaluator.Transaction
import com.twitter.querulous.query
import com.twitter.querulous.query.{QueryClass => QuerulousQueryClass, SqlQueryTimeoutException}
import com.twitter.util.{Time, Future}
import com.twitter.util.TimeConversions._
import com.mysql.jdbc.exceptions.MySQLTransactionRollbackException
import State._

object QueryClass {
  val Select       = QuerulousQueryClass.Select
  val Execute      = QuerulousQueryClass.Execute
  val SelectSingle = QuerulousQueryClass("select_single")
  val SelectModify = QuerulousQueryClass("select_modify")
  val SelectCopy   = QuerulousQueryClass("select_copy")
  val SelectIntersection         = QuerulousQueryClass("select_intersection")
  val SelectIntersectionSmall    = QuerulousQueryClass("select_intersection_small")
  val SelectMetadata             = QuerulousQueryClass("select_metadata")
}

class SqlShardFactory(
  instantiatingQueryEvaluatorFactory: AsyncQueryEvaluatorFactory,
  lowLatencyQueryEvaluatorFactory: AsyncQueryEvaluatorFactory,
  materializingQueryEvaluatorFactory: AsyncQueryEvaluatorFactory,
  connection: Connection)
extends ShardFactory[Shard] {

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
) ENGINE=INNODB"""

  val METADATA_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS %s (
  source_id             %s                       NOT NULL,
  count                 INT                      NOT NULL,
  state                 TINYINT                  NOT NULL,
  updated_at            INT UNSIGNED             NOT NULL,

  PRIMARY KEY (source_id)
) ENGINE=INNODB
"""

  def instantiate(shardInfo: ShardInfo, weight: Int) = {
    val queryEvaluator = instantiatingQueryEvaluatorFactory(connection.withHost(shardInfo.hostname))
    val lowLatencyQueryEvaluator = lowLatencyQueryEvaluatorFactory(connection.withHost(shardInfo.hostname))
    new SqlExceptionWrappingProxyFactory[Shard](shardInfo.id).apply(
      new SqlShard(shardInfo, queryEvaluator, lowLatencyQueryEvaluator, deadlockRetries)
    )
  }

  //XXX: enforce readonly connection
  def instantiateReadOnly(shardInfo: ShardInfo, weight: Int) = instantiate(shardInfo, weight)

  def materialize(shardInfo: ShardInfo) = {
    try {
      val queryEvaluator = materializingQueryEvaluatorFactory(connection.withHost(shardInfo.hostname).withoutDatabase)

      val tableNamePrefix = connection.database + "." + shardInfo.tablePrefix
      val createDB        = "CREATE DATABASE IF NOT EXISTS " + connection.database
      val edgeDDL         = EDGE_TABLE_DDL.format(tableNamePrefix +"_edges", shardInfo.sourceType, shardInfo.destinationType)
      val metadataDDL     = METADATA_TABLE_DDL.format(tableNamePrefix +"_metadata", shardInfo.sourceType)

      val future = queryEvaluator.execute(createDB) flatMap { _ =>
        queryEvaluator.execute(edgeDDL) flatMap { _ =>
          queryEvaluator.execute(metadataDDL)
        }
      }

      future()

    } catch {
      case e: SQLException => throw new ShardException(e.toString)
      case e: SqlQueryTimeoutException => throw new ShardTimeoutException(e.timeout, shardInfo.id, e)
    }
  }
}

/**
 * All methods are externally asynchronous via Futures, but Transactions are only available in a
 * context where it is safe to block (a FuturePool), so private methods may take Transactions with
 * the understanding that they will be executed in a blocking fashion.
 */
class SqlShard(
  shardInfo: ShardInfo,
  queryEvaluator: AsyncQueryEvaluator,
  lowLatencyQueryEvaluator: AsyncQueryEvaluator,
  deadlockRetries: Int)
extends Shard {

  private val tablePrefix = shardInfo.tablePrefix
  private val randomGenerator = new Random

  type EdgeStateChange = (Option[State],State)
  import QueryClass._

  def get(sourceId: Long, destinationId: Long) = {
    lowLatencyQueryEvaluator.selectOne(SelectSingle, "SELECT * FROM " + tablePrefix + "_edges WHERE source_id = ? AND destination_id = ?", sourceId, destinationId) { row =>
      makeEdge(row)
    }
  }

  def getMetadata(sourceId: Long) = {
    lowLatencyQueryEvaluator.selectOne(SelectMetadata, "SELECT * FROM " + tablePrefix + "_metadata WHERE source_id = ?", sourceId) { row =>
      new Metadata(sourceId, State(row.getInt("state")), row.getInt("count"), Time.fromSeconds(row.getInt("updated_at")))
    }
  }

  def getMetadataForWrite(sourceId: Long) = {
    queryEvaluator.selectOne(SelectMetadata, "SELECT * FROM " + tablePrefix + "_metadata WHERE source_id = ?", sourceId) { row =>
      new Metadata(sourceId, State(row.getInt("state")), row.getInt("count"), Time.fromSeconds(row.getInt("updated_at")))
    }
  }

  def selectAllMetadata(cursor: Cursor, count: Int) = {
    val metadatas = new mutable.ArrayBuffer[Metadata]
    var nextCursor = Cursor.Start
    var returnedCursor = Cursor.End

    var i = 0
    val query = "SELECT * FROM " + tablePrefix +
      "_metadata WHERE source_id > ? ORDER BY source_id LIMIT ?"

    val f = queryEvaluator.select(SelectCopy, query, cursor.position, count + 1) { row =>
      if (i < count) {
        val sourceId = row.getLong("source_id")
        metadatas += new Metadata(sourceId, State(row.getInt("state")), row.getInt("count"),
                              Time.fromSeconds(row.getInt("updated_at")))
        nextCursor = Cursor(sourceId)
        i += 1
      } else {
        returnedCursor = nextCursor
      }
    }

    f map { _ => (metadatas, returnedCursor) }
  }

  def count(sourceId: Long, states: Seq[State]) = {
    val f = lowLatencyQueryEvaluator.selectOne(SelectMetadata, "SELECT state, `count` FROM " + tablePrefix + "_metadata WHERE source_id = ?", sourceId) { row =>
      states.foldLeft(0) { (result, state) =>
        result + (if (state == State(row.getInt("state"))) row.getInt("count") else 0)
      }
    }

    f flatMap {
      _ map (Future.value(_)) getOrElse {
        populateMetadata(sourceId, Normal)
        count(sourceId, states)
      }
    }
  }

  private def populateMetadata(sourceId: Long, state: State): Future[Unit] =
    populateMetadata(sourceId, state, Time.epoch)

  /** TODO: bulk insert? */
  private def populateMetadata(sourceId: Long, state: State, updatedAt: Time): Future[Unit] = {
    val f = computeCount(sourceId, state) flatMap { count =>
      queryEvaluator.execute(
        "INSERT INTO " + tablePrefix + "_metadata (source_id, count, state, updated_at) VALUES (?, ?, ?, ?)",
        sourceId,
        count,
        state.id,
        updatedAt.inSeconds)
    }
    f.unit handle {
      case e: SQLIntegrityConstraintViolationException => ()
    }
  }

  private def computeCount(transaction: Transaction, sourceId: Long, state: State): Int = {
    transaction.count("SELECT count(*) FROM " + tablePrefix + "_edges WHERE source_id = ? AND state = ?", sourceId, state.id)
  }

  private def computeCount(sourceId: Long, state: State): Future[Int] = {
    queryEvaluator.transaction { computeCount(_, sourceId, state) }
  }

  def selectAll(cursor: (Cursor, Cursor), count: Int) = {
    val edges = new mutable.ArrayBuffer[Edge]
    var nextCursor = (Cursor.Start, Cursor.Start)
    var returnedCursor = (Cursor.End, Cursor.End)

    var i = 0
    val query = "SELECT * FROM " + tablePrefix + "_edges " +
      "USE INDEX (unique_source_id_destination_id) WHERE (source_id = ? AND destination_id > ?) " +
      "OR (source_id > ?) ORDER BY source_id, destination_id LIMIT ?"
    val (cursor1, cursor2) = cursor
    val f = queryEvaluator.select(SelectCopy, query, cursor1.position, cursor2.position, cursor1.position,
                          count + 1) { row =>
      if (i < count) {
        edges += makeEdge(row)
        nextCursor = (Cursor(row.getLong("source_id")), Cursor(row.getLong("destination_id")))
        i += 1
      } else {
        returnedCursor = nextCursor
      }
    }

    f map { _ => (edges, returnedCursor) }
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
                     cursor: Cursor, conditions: String, args: Any*): Future[ResultWindow[Long]] = {
    select(QueryClass.Select, cursorName, index, count, cursor, conditions, args: _*)
  }

  private def select(queryClass: QuerulousQueryClass, cursorName: String, index: String, count: Int,
                     cursor: Cursor, conditions: String, args: Any*): Future[ResultWindow[Long]] = {
    val order = if (cursor < Cursor.Start) "ASC" else "DESC"
    val inequality = if (order == "DESC") "<" else ">"

    val (continueCursorQuery, args1) = query(cursorName, index, 1, cursor, opposite(order), opposite(inequality), conditions, args)
    val (edgesQuery, args2) = query(cursorName, index, count + 1, cursor, order, inequality, conditions, args)
    val totalQuery = continueCursorQuery + " UNION ALL " + edgesQuery
    queryEvaluator.select(queryClass, totalQuery, args1 ++ args2: _*) { row =>
      (row.getLong("destination_id") -> Cursor(row.getLong(cursorName)))
    } map { edges =>
      var page = edges.view
      if (cursor < Cursor.Start) page = page.reverse
      new ResultWindow(page, count, cursor)
    }
  }

  def selectEdges(sourceId: Long, states: Seq[State], count: Int, cursor: Cursor) = {
    val conditions = "source_id = ? AND state IN (?)"
    val order = if (cursor < Cursor.Start) "ASC" else "DESC"
    val inequality = if (order == "DESC") "<" else ">"
    val args = sourceId :: states.map(_.id).toList
    val (edgesQuery, args1) = query("*", "position", "PRIMARY", count + 1, cursor, order, inequality, conditions, args)
    val (continueCursorQuery, args2) = query("*", "position", "PRIMARY", 1, cursor, opposite(order), opposite(inequality), conditions, args)

    queryEvaluator.select(continueCursorQuery + " UNION ALL " + edgesQuery, args1 ++ args2: _*) { row =>
      (makeEdge(row) -> Cursor(row.getLong("position")))
    } map { edges =>
      var page = edges.view
      if (cursor < Cursor.Start) page = page.reverse
      new ResultWindow(page, count, cursor)
    }
  }

  private def query(cursorName: String, index: String, count: Int, cursor: Cursor, order: String,
                    inequality: String, conditions: String, args: Seq[Any]): (String, Seq[Any]) = {
    val projections = Set("destination_id", cursorName).mkString(", ")
    query(projections, cursorName, index, count, cursor, order, inequality, conditions, args)
  }

  private def query(projections: String, cursorName: String, index: String, count: Int,
                    cursor: Cursor, order: String, inequality: String, conditions: String, args: Seq[Any]): (String, Seq[Any]) = {
    val position = if (cursor == Cursor.Start) Long.MaxValue else cursor.magnitude.position

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
    if (destinationIds.size == 0) Future.value(Nil) else {
      val (evaluator, queryClass) = destinationIds.size match {
        case s if s == 1 => (lowLatencyQueryEvaluator, SelectSingle)
        case s if s <= 50 => (lowLatencyQueryEvaluator, SelectIntersectionSmall)
        case s => (queryEvaluator, SelectIntersection)
      }
      evaluator.select(queryClass, "SELECT destination_id FROM " + tablePrefix + "_edges USE INDEX (unique_source_id_destination_id) WHERE source_id = ? AND state IN (?) AND destination_id IN (?) ORDER BY destination_id DESC",
        sourceId, states.map(_.id), destinationIds) { row =>
        row.getLong("destination_id")
      }
    }
  }

  def intersectEdges(sourceId: Long, states: Seq[State], destinationIds: Seq[Long]) = {
    if (destinationIds.size == 0) Future.value(Nil) else {
      val (evaluator, queryClass) = destinationIds.size match {
        case s if s == 1 => (lowLatencyQueryEvaluator, SelectSingle)
        case s if s <= 50 => (lowLatencyQueryEvaluator, SelectIntersectionSmall)
        case s => (queryEvaluator, SelectIntersection)
      }
      evaluator.select(queryClass, "SELECT * FROM " + tablePrefix + "_edges USE INDEX (unique_source_id_destination_id) WHERE source_id = ? AND state IN (?) AND destination_id IN (?) ORDER BY destination_id DESC",
        sourceId, states.map(_.id), destinationIds) { row =>
        makeEdge(row)
      }
    }
  }

  def add(sourceId: Long, destinationId: Long, position: Long, updatedAt: Time) = {
    write(new Edge(sourceId, destinationId, position, updatedAt, 0, Normal))
  }

  def add(sourceId: Long, updatedAt: Time) = {
    updateMetadata(sourceId, Normal, updatedAt)
  }

  def negate(sourceId: Long, destinationId: Long, position: Long, updatedAt: Time) = {
    write(new Edge(sourceId, destinationId, position, updatedAt, 0, Negative))
  }

  def negate(sourceId: Long, updatedAt: Time) = {
    updateMetadata(sourceId, Negative, updatedAt)
  }

  def remove(sourceId: Long, destinationId: Long, position: Long, updatedAt: Time) = {
    write(new Edge(sourceId, destinationId, position, updatedAt, 0, Removed))
  }

  def remove(sourceId: Long, updatedAt: Time) = {
    updateMetadata(sourceId, Removed, updatedAt)
  }

  def archive(sourceId: Long, destinationId: Long, position: Long, updatedAt: Time) = {
    write(new Edge(sourceId, destinationId, position, updatedAt, 0, Archived))
  }

  def archive(sourceId: Long, updatedAt: Time) = {
    updateMetadata(sourceId, Archived, updatedAt)
  }

  override def equals(other: Any) = {
    error("called!")
    false
  }

  override def hashCode = {
    (if (tablePrefix == null) 37 else tablePrefix.hashCode * 37) + (if(queryEvaluator == null) 1 else queryEvaluator.hashCode)
  }


  private def insertEdge(transaction: Transaction, edge: Edge): EdgeStateChange = {
    transaction.execute("INSERT INTO " + tablePrefix + "_edges (source_id, position, " +
                        "updated_at, destination_id, count, state) VALUES (?, ?, ?, ?, ?, ?)",
                        edge.sourceId, edge.position, edge.updatedAt.inSeconds,
                        edge.destinationId, edge.count, edge.state.id)
    (None, edge.state)
  }

  def bulkUnsafeInsertEdges(edges: Seq[Edge]): Future[Unit] = {
    queryEvaluator.transaction { transaction =>
      bulkUnsafeInsertEdges(transaction, edges)
    }
  }

  private def bulkUnsafeInsertEdges(transaction: Transaction, edges: Seq[Edge]) = {
    if (edges.size > 0) {
      val query = "INSERT INTO " + tablePrefix + "_edges (source_id, position, updated_at, destination_id, count, state) VALUES (?, ?, ?, ?, ?, ?)"
      transaction.executeBatch(query) { batch =>
        edges.foreach { edge =>
          batch(edge.sourceId, edge.position, edge.updatedAt.inSeconds, edge.destinationId, edge.count, edge.state.id)
        }
      }
    }
  }

  def bulkUnsafeInsertMetadata(metadatas: Seq[Metadata]): Future[Unit] = {
    if (metadatas.length <= 0)
        Future.Unit
    else {
      val query = "INSERT INTO " + tablePrefix + "_metadata (source_id, count, state, updated_at) VALUES (?, ?, ?, ?)"
      queryEvaluator.executeBatch(query) { batch =>
        metadatas.foreach { metadata =>
          batch(metadata.sourceId, metadata.count, metadata.state.id, metadata.updatedAt.inSeconds)
        }
      }.unit
    }
  }

  private def updateEdge(transaction: Transaction, edge: Edge, oldEdge: Edge): EdgeStateChange = {
    if ((oldEdge.updatedAtSeconds == edge.updatedAtSeconds) && (oldEdge.state max edge.state) != edge.state)
      return (Some(oldEdge.state), oldEdge.state)

    val updatedRows = if (
      oldEdge.state != Archived &&  // Only update position when coming from removed or negated into normal
      oldEdge.state != Normal &&
      edge.state == Normal
    ) {
      transaction.execute("UPDATE " + tablePrefix + "_edges SET updated_at = ?, " +
                          "position = ?, count = 0, state = ? " +
                          "WHERE source_id = ? AND destination_id = ? AND " +
                          "updated_at <= ? LIMIT 1",
                          edge.updatedAt.inSeconds, edge.position, edge.state.id,
                          edge.sourceId, edge.destinationId, edge.updatedAt.inSeconds)
    } else {
      try {
        transaction.execute("UPDATE " + tablePrefix + "_edges SET updated_at = ?, " +
                            "count = 0, state = ? " +
                            "WHERE source_id = ? AND destination_id = ? AND updated_at <= ? LIMIT 1",
                            edge.updatedAt.inSeconds, edge.state.id, edge.sourceId,
                            edge.destinationId, edge.updatedAt.inSeconds)
      } catch {
        case e: SQLIntegrityConstraintViolationException =>
          // usually this is a (source_id, state, position) violation. scramble the position more.
          // FIXME: hacky. remove with the new schema.
          transaction.execute("UPDATE " + tablePrefix + "_edges SET updated_at = ?, " +
                              "count = 0, state = ?, position = position + ? " +
                              "WHERE source_id = ? AND destination_id = ? AND updated_at <= ? LIMIT 1",
                              edge.updatedAt.inSeconds, edge.state.id,
                              (randomGenerator.nextInt() % 999) + 1, edge.sourceId,
                              edge.destinationId, edge.updatedAt.inSeconds)
      }
    }

    val newEdgeState =
      updatedRows match {
        case 1 => edge.state
        case 0 => oldEdge.state
        case x =>
          throw new AssertionError("Invalid update count " + x + ": LIMIT statement not applied?")
      }
    (Some(oldEdge.state), newEdgeState)
  }

  // returns the old and new edge states. `predictExistence`=true for normal
  // operations, false for copy/migrate
  private def writeEdge(transaction: Transaction, metadata: Metadata, edge: Edge,
                        predictExistence: Boolean): EdgeStateChange = {
    if (predictExistence) {
      transaction.selectOne(SelectModify,
                            "SELECT * FROM " + tablePrefix + "_edges WHERE source_id = ? " +
                            "and destination_id = ?", edge.sourceId, edge.destinationId) { row =>
        makeEdge(row)
      }.map { oldRow =>
        updateEdge(transaction, edge, oldRow)
      }.getOrElse {
        insertEdge(transaction, edge)
      }
    } else {
      try {
        insertEdge(transaction, edge)
      } catch {
        case e: SQLIntegrityConstraintViolationException =>
          transaction.selectOne(SelectModify,
                                "SELECT * FROM " + tablePrefix + "_edges WHERE source_id = ? " +
                                "and destination_id = ?", edge.sourceId, edge.destinationId) { row =>
            makeEdge(row)
          }.map { oldRow =>
            updateEdge(transaction, edge, oldRow)
          }.getOrElse {
            // edge removed within transaction: nothing obvious to do
            throw new RuntimeException("Edge disappeared during transaction?", e)
          }
      }
    }
  }

  private def write(edge: Edge): Future[Unit] = {
    write(edge, deadlockRetries, true)
  }

  private def write(edge: Edge, tries: Int, predictExistence: Boolean): Future[Unit] = {
    try {
      atomically(edge.sourceId) { (transaction, metadata) =>
        val preAndPostStates = writeEdge(transaction, metadata, edge, predictExistence)
        val countDelta = countDeltaFor(preAndPostStates, metadata.state)
        if (countDelta != 0) {
          transaction.execute("UPDATE " + tablePrefix + "_metadata SET count = GREATEST(count + ?, 0) " +
                              "WHERE source_id = ?", countDelta, edge.sourceId)
        }
      }
    } catch {
      case e: MySQLTransactionRollbackException if (tries > 0) =>
        write(edge, tries - 1, predictExistence)
      case e: SQLIntegrityConstraintViolationException if (tries > 0) =>
        // temporary. until the position differential between master/slave is fixed, it's
        // possible for a slave migration to have two different edges with the same position.
        write(new Edge(edge.sourceId, edge.destinationId, edge.position + 1, edge.updatedAt,
                       edge.count, edge.state), tries - 1, predictExistence)
    }
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
        e.getUpdateCounts.zip(edges).foreach { case (errorCode, edge) =>
          if (errorCode < 0) {
            failed += edge
          } else {
            completed += edge
          }
        }
        BurstResult(completed, failed)
    }
  }

  private def countDeltaFor(oldAndNewEdgeState: EdgeStateChange, metadataState: State): Int =
    oldAndNewEdgeState match {
      case (None, `metadataState`) => 1
      case (Some(o), n) if o == n => 0
      case (Some(_), `metadataState`) => 1
      case (Some(`metadataState`), _) => -1
      case (_, _) => 0
    }

  private def updateCount(transaction: Transaction, sourceId: Long, countDelta: Int) = {
    transaction.execute("UPDATE " + tablePrefix + "_metadata SET count = count + ? " +
                        "WHERE source_id = ?", countDelta, sourceId)
  }

  def writeCopies(edges: Seq[Edge]) = {
    if (!edges.isEmpty) {
      Stats.addMetric("copy-burst", edges.size)

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
          Stats.incr("copy-fallback")
          var currentSourceId = -1L
          var countDelta = 0
          result.failed.foreach { edge =>
            if (edge.sourceId != currentSourceId) {
              if (currentSourceId != -1)
                updateCount(transaction, currentSourceId, countDelta)
              currentSourceId = edge.sourceId
              countDelta = 0
            }
            val metadataForId = metadataById(edge.sourceId)
            val preAndPostStates = writeEdge(transaction, metadataForId, edge, false)
            countDelta += countDeltaFor(preAndPostStates, metadataForId.state)
          }
          updateCount(transaction, currentSourceId, countDelta)
        }
      }
    } else {
      // Nothing to do.
      Future.Unit
    }
  }

  private def atomically[A](sourceId: Long)(f: (Transaction, Metadata) => A): Future[A] = {
    atomically(Seq(sourceId)) { (t, map) => f(t, map(sourceId)) }
  }

  private def atomically[A](sourceIds: Seq[Long])(f: (Transaction, Map[Long, Metadata]) => A): Future[A] = {
    queryEvaluator.transaction { transaction =>

      val mdMapBuilder = Map.newBuilder[Long, Metadata]

      transaction.select(
        SelectModify,
        "SELECT * FROM " + tablePrefix + "_metadata WHERE source_id in (?) FOR UPDATE",
        sourceIds
      ) { row =>
        val md = new Metadata(
          row.getLong("source_id"),
          State(row.getInt("state")),
          row.getInt("count"),
          Time.fromSeconds(row.getInt("updated_at"))
        )

        mdMapBuilder += (row.getLong("source_id") -> md)
      }

      val mdMap = mdMapBuilder.result

      if (mdMap.size < sourceIds.length) {
        Left(sourceIds filterNot { mdMap contains _ })
      } else {
        Right(f(transaction, mdMap))
      }
    } flatMap {
      case Left(missingMeta) =>
        // insert metadata in parallel, then recurse to retry TODO: termination?
        Future.join(missingMeta map { populateMetadata(_, Normal) }) flatMap { _ =>
          atomically(sourceIds)(f)
        }
      case Right(rv) => Future.value(rv)
    }
  }

  def writeMetadata(metadata: Metadata): Future[Unit] = {
    val insertFuture = queryEvaluator.execute("INSERT INTO " + tablePrefix + "_metadata (source_id, count, state, " +
      "updated_at) VALUES (?, ?, ?, ?)",
      metadata.sourceId, 0, metadata.state.id, metadata.updatedAt.inSeconds).unit
    insertFuture rescue {
      case e: SQLIntegrityConstraintViolationException =>
          atomically(metadata.sourceId) { (transaction, oldMetadata) =>
          transaction.execute("UPDATE " + tablePrefix + "_metadata SET state = ?, updated_at = ? " +
                              "WHERE source_id = ? AND updated_at <= ?",
                              metadata.state.id, metadata.updatedAt.inSeconds, metadata.sourceId,
                              metadata.updatedAt.inSeconds)
        }
    }
  }

  def writeMetadatas(metadatas: Seq[Metadata]): Future[Unit] = {
    if (metadatas.isEmpty)
      Future.Unit
    else {
      val query = "INSERT INTO " + tablePrefix + "_metadata (source_id, count, state, updated_at) VALUES (?, 0, ?, ?)"
      queryEvaluator.executeBatch(query) { batch =>
        metadatas.foreach { metadata =>
          batch(metadata.sourceId, metadata.state.id, metadata.updatedAt.inSeconds)
        }
      }.unit.rescue {
        case e: BatchUpdateException =>
          val retryMetadata = e.getUpdateCounts.zip(metadatas).collect {
            case (errorCode, metadata) if errorCode < 0 => metadata
          }

          def loop(left: Seq[Metadata]): Future[Unit] = if (left.isEmpty) {
            Future.Done
          } else {
            writeMetadata(left.head) flatMap { _ => loop(left.tail) }
          }

          loop(retryMetadata)
      }
    }
  }

  def updateMetadata(metadata: Metadata): Future[Unit] = updateMetadata(metadata.sourceId, metadata.state, metadata.updatedAt)

  // FIXME: computeCount could be really expensive. :(
  def updateMetadata(sourceId: Long, state: State, updatedAt: Time) = {
    atomically(sourceId) { (transaction, metadata) =>
      if ((updatedAt.inSeconds != metadata.updatedAtSeconds) || ((metadata.state max state) == state)) {
        transaction.execute("UPDATE " + tablePrefix + "_metadata SET state = ?, updated_at = ?, count = ? WHERE source_id = ? AND updated_at <= ?",
          state.id, updatedAt.inSeconds, computeCount(transaction, sourceId, state), sourceId, updatedAt.inSeconds)
      }
    } map { _ => }
  }

  private def makeEdge(sourceId: Long, destinationId: Long, position: Long, updatedAt: Time, count: Int, stateId: Int): Edge = {
    new Edge(sourceId, destinationId, position, updatedAt, count, State(stateId))
  }

  private def makeEdge(row: ResultSet): Edge = {
    makeEdge(row.getLong("source_id"), row.getLong("destination_id"), row.getLong("position"), Time.fromSeconds(row.getInt("updated_at")), row.getInt("count"), row.getInt("state"))
  }
}
