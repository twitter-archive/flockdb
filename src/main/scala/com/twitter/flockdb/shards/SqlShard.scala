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

import java.sql.{ResultSet, SQLException, SQLIntegrityConstraintViolationException}
import scala.collection.mutable
import com.twitter.gizzard.proxy.SqlExceptionWrappingProxy
import com.twitter.gizzard.shards
import com.twitter.results.{Cursor, ResultWindow}
import com.twitter.querulous.evaluator.{QueryEvaluator, QueryEvaluatorFactory, Transaction}
import com.twitter.querulous.query.SqlQueryTimeoutException
import com.twitter.xrayspecs.Time
import com.twitter.xrayspecs.TimeConversions._
import com.mysql.jdbc.exceptions.MySQLTransactionRollbackException
import net.lag.configgy.ConfigMap
import net.lag.logging.Logger
import State._


class SqlShardFactory(instantiatingQueryEvaluatorFactory: QueryEvaluatorFactory, materializingQueryEvaluatorFactory: QueryEvaluatorFactory, config: ConfigMap)
  extends shards.ShardFactory[Shard] {

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
    val queryEvaluator = instantiatingQueryEvaluatorFactory(List(shardInfo.hostname), config("edges.db_name"), config("db.username"), config("db.password"))
    SqlExceptionWrappingProxy[Shard](new SqlShard(queryEvaluator, shardInfo, weight, children, config))
  }

  def materialize(shardInfo: shards.ShardInfo) = {
    try {
      val queryEvaluator = materializingQueryEvaluatorFactory(
        List(shardInfo.hostname),
        null,
        config("db.username"),
        config("db.password"))
      queryEvaluator.execute("CREATE DATABASE IF NOT EXISTS " + config("edges.db_name"))
      queryEvaluator.execute(EDGE_TABLE_DDL.format(config("edges.db_name") + "." + shardInfo.tablePrefix + "_edges", shardInfo.sourceType, shardInfo.destinationType))
      queryEvaluator.execute(METADATA_TABLE_DDL.format(config("edges.db_name") + "." + shardInfo.tablePrefix + "_metadata", shardInfo.sourceType))
    } catch {
      case e: SQLException => throw new shards.ShardException(e.toString)
      case e: SqlQueryTimeoutException => throw new shards.ShardTimeoutException(e.timeout, e)
    }
  }
}


class SqlShard(private val queryEvaluator: QueryEvaluator, val shardInfo: shards.ShardInfo,
               val weight: Int, val children: Seq[Shard], config: ConfigMap) extends Shard {
  val log = Logger.get(getClass.getName)
  private val tablePrefix = shardInfo.tablePrefix
  private val randomGenerator = new util.Random

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
    queryEvaluator.select("SELECT * FROM " + tablePrefix + "_metadata WHERE source_id > ? ORDER BY source_id LIMIT ?", cursor.position, count + 1) { row =>
      if (i < count) {
        val sourceId = row.getLong("source_id")
        metadatas += Metadata(sourceId, State(row.getInt("state")), row.getInt("count"), Time(row.getInt("updated_at").seconds))
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
    queryEvaluator.select("SELECT * FROM " + tablePrefix + "_edges USE INDEX (unique_source_id_destination_id) WHERE (source_id = ? AND destination_id > ?) OR (source_id > ?) ORDER BY source_id, destination_id LIMIT ?", cursor._1.position, cursor._2.position, cursor._1.position, count + 1) { row =>
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

  def selectByDestinationId(sourceId: Long, states: Seq[State], count: Int, cursor: Cursor) = {
    select("destination_id", "unique_source_id_destination_id", count, cursor,
      "source_id = ? AND state IN (?)",
      List(sourceId, states.map(_.id).toList): _*)
  }

  def selectIncludingArchived(sourceId: Long, count: Int, cursor: Cursor) = {
    select("destination_id", "unique_source_id_destination_id", count, cursor,
      "source_id = ? AND state != ?",
      sourceId, Removed.id)
  }

  def selectByPosition(sourceId: Long, states: Seq[State], count: Int, cursor: Cursor) = {
    select("position", "PRIMARY", count, cursor,
      "source_id = ? AND state IN (?)",
      List(sourceId, states.map(_.id).toList): _*)
  }

  private def select(cursorName: String, index: String, count: Int, cursor: Cursor, conditions: String, args: Any*) = {
    var edges = new mutable.ArrayBuffer[(Long, Cursor)]
    val order = if (cursor < Cursor.Start) "ASC" else "DESC"
    val inequality = if (order == "DESC") "<" else ">"

    val (continueCursorQuery, args1) = query(cursorName, index, 1, cursor, opposite(order), opposite(inequality), conditions, args)
    val (edgesQuery, args2)  = query(cursorName, index, count + 1, cursor, order, inequality, conditions, args)
    queryEvaluator.select(continueCursorQuery + " UNION " + edgesQuery, args1 ++ args2: _*) { row =>
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
    write(new Edge(sourceId, destinationId, position, updatedAt, 1, Normal))
  }

  def add(sourceId: Long, updatedAt: Time) {
    updateMetadata(sourceId, Normal, updatedAt)
  }

  def negate(sourceId: Long, destinationId: Long, position: Long, updatedAt: Time) = {
    write(new Edge(sourceId, destinationId, position, updatedAt, 1, Negative))
  }

  def negate(sourceId: Long, updatedAt: Time) {
    updateMetadata(sourceId, Negative, updatedAt)
  }

  def remove(sourceId: Long, destinationId: Long, position: Long, updatedAt: Time) = {
    write(new Edge(sourceId, destinationId, position, updatedAt, 1, Removed))
  }

  def remove(sourceId: Long, updatedAt: Time) {
    updateMetadata(sourceId, Removed, updatedAt)
  }

  def archive(sourceId: Long, destinationId: Long, position: Long, updatedAt: Time) = {
    write(new Edge(sourceId, destinationId, position, updatedAt, 1, Archived))
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

  override def hashCode = tablePrefix.hashCode * 37 + queryEvaluator.hashCode

  private class MissingMetadataRow extends Exception("Missing Count Row")

  private def insertEdge(transaction: Transaction, metadata: Metadata, edge: Edge): Int = {
    val insertedRows =
      transaction.execute("INSERT INTO " + tablePrefix + "_edges (source_id, position, " +
                          "updated_at, destination_id, count, state) VALUES (?, ?, ?, ?, ?, ?)",
                          edge.sourceId, edge.position, edge.updatedAt.inSeconds,
                          edge.destinationId, edge.count, edge.state.id)
    if (edge.state == metadata.state) insertedRows else 0
  }

  private def updateEdge(transaction: Transaction, metadata: Metadata, edge: Edge,
                         oldEdge: Edge): Int = {
    if ((oldEdge.updatedAt == edge.updatedAt) && (oldEdge.state max edge.state) != edge.state) return 0

    val updatedRows = if (oldEdge.state != Archived && edge.state == Normal) {
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

  // returns +1, 0, or -1, depending on how the metadata count should change after this operation.
  // `predictExistence`=true for normal operations, false for copy/migrate.
  private def writeEdge(transaction: Transaction, metadata: Metadata, edge: Edge,
                        predictExistence: Boolean): Int = {
    val countDelta = if (predictExistence) {
      transaction.selectOne("SELECT * FROM " + tablePrefix + "_edges WHERE source_id = ? " +
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
          transaction.selectOne("SELECT * FROM " + tablePrefix + "_edges WHERE source_id = ? " +
                                "and destination_id = ?", edge.sourceId, edge.destinationId) { row =>
            makeEdge(row)
          }.map { oldRow =>
            updateEdge(transaction, metadata, edge, oldRow)
          }.getOrElse(0)
      }
    }
    if (edge.state == metadata.state) countDelta else -countDelta
  }

  private def write(edge: Edge) {
    write(edge, config("errors.deadlock_retries").toInt, true)
  }

  private def write(edge: Edge, tries: Int, predictExistence: Boolean) {
    try {
      atomically(edge.sourceId) { (transaction, metadata) =>
        val countDelta = writeEdge(transaction, metadata, edge, predictExistence)
        if (countDelta != 0) {
          transaction.execute("UPDATE " + tablePrefix + "_metadata SET count = count + ? " +
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

  def writeCopies(edges: Seq[Edge]) {
    val retries = config("errors.deadlock_retries").toInt
    var remaining = edges
    while (remaining.size > 0) {
      val burst = new mutable.ArrayBuffer[Edge]
      val currentSourceId = remaining(0).sourceId
      var index = 0
      while (remaining.size > index && remaining(index).sourceId == currentSourceId) {
        burst += remaining(index)
        index += 1
      }

      var countDelta = 0
      atomically(currentSourceId) { (transaction, metadata) =>
        try {
          burst.foreach { edge =>
            countDelta += writeEdge(transaction, metadata, edge, false)
          }
        } finally {
          if (countDelta != 0) {
            transaction.execute("UPDATE " + tablePrefix + "_metadata SET count = count + ? " +
                                "WHERE source_id = ?", countDelta, currentSourceId)
          }
        }
      }

      remaining = remaining.drop(index)
    }
  }

  def withLock[A](sourceId: Long)(f: (Shard, Metadata) => A) = {
    atomically(sourceId) { (transaction, metadata) =>
      f(new SqlShard(transaction, shardInfo, weight, children, config), metadata)
    }
  }

  private def atomically[A](sourceId: Long)(f: (Transaction, Metadata) => A): A = {
    try {
      queryEvaluator.transaction { transaction =>
        transaction.selectOne("SELECT * FROM " + tablePrefix + "_metadata WHERE source_id = ? FOR UPDATE", sourceId) { row =>
          f(transaction, Metadata(sourceId, State(row.getInt("state")), row.getInt("count"), Time(row.getInt("updated_at").seconds)))
        } getOrElse(throw new MissingMetadataRow)
      }
    } catch {
      case e: MissingMetadataRow =>
        populateMetadata(sourceId, Normal)
        atomically(sourceId)(f)
    }
  }

  def writeMetadata(metadata: List[Metadata])  = {
    def update_value_if_newer(column:String) = column + "=IF(updated_at <= VALUES(updated_at), VALUES(" + column + "), " + column + ")"

    val query = "INSERT INTO " + tablePrefix + "_metadata " +
                "(source_id, count, state, updated_at) VALUES " +
                (1 to metadata.length).map(_ =>"(?, ?, ?, ?)").mkString(", ") +
                " ON DUPLICATE KEY UPDATE " +
                update_value_if_newer("state") + " , " +
                update_value_if_newer("updated_at")
    val params = metadata.map(m => List(m.sourceId, 0, m.state.id, m.updatedAt.inSeconds)).flatten: List[Any]

    queryEvaluator.execute(query, params: _*)
  }

  def writeMetadata(metadata: Metadata) = this.writeMetadata(List(metadata))

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

  private def makeEdge(sourceId: Long, destinationId: Long, position: Long, updatedAt: Time, count: Int, stateId: Int): Edge = {
    new Edge(sourceId, destinationId, position, updatedAt, count, State(stateId))
  }

  private def makeEdge(row: ResultSet): Edge = {
    makeEdge(row.getLong("source_id"), row.getLong("destination_id"), row.getLong("position"), Time(row.getInt("updated_at").seconds), row.getInt("count"), row.getInt("state"))
  }
}
