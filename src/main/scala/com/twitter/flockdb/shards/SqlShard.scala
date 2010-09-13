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
  state                 TINYINT                  NOT NULL,

  PRIMARY KEY (source_id, state, position),

  UNIQUE unique_source_id_destination_id (source_id, destination_id)
) TYPE=INNODB"""

  val METADATA_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS %s (
  source_id             %s                       NOT NULL,
  count0                INT                      NOT NULL DEFAULT 0,
  count1                INT                      NOT NULL DEFAULT 0,
  count2                INT                      NOT NULL DEFAULT 0,
  count3                INT                      NOT NULL DEFAULT 0,
  state                 TINYINT                  NOT NULL DEFAULT 0,
  updated_at            INT UNSIGNED             NOT NULL DEFAULT 0,
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
      Metadata(sourceId, State(row.getInt("state")), row.getInt("count0"), row.getInt("count1"), row.getInt("count2"), row.getInt("count3"), Time(row.getInt("updated_at").seconds))
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
        metadatas += Metadata(sourceId, State(row.getInt("state")), row.getInt("count0"), row.getInt("count1"), row.getInt("count2"), row.getInt("count3"), Time(row.getInt("updated_at").seconds))
        nextCursor = Cursor(sourceId)
        i += 1
      } else {
        returnedCursor = nextCursor
      }
    }

    (metadatas, returnedCursor)
  }

  def count(sourceId: Long, states: Seq[State]): Int = {
    queryEvaluator.selectOne("SELECT * FROM " + tablePrefix + "_metadata WHERE source_id = ?", sourceId) { row =>
      states.foldLeft(0) { (result, state) =>
        result + row.getInt("count"+state.id)
      }
    } getOrElse {
      initializeMetadata(sourceId)
      0
    }
  }

  def counts(sourceIds: Seq[Long], results: mutable.Map[Long, Int]) {
    queryEvaluator.select("SELECT * FROM " + tablePrefix + "_metadata WHERE source_id IN (?)", sourceIds) { row =>
      val state = row.getInt("state")
      results(row.getLong("source_id")) = row.getInt("count" + state)
    }
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
    write(Seq(Edge(sourceId, destinationId, position, updatedAt, Normal)))
  }

  def add(sourceId: Long, updatedAt: Time) {
    updateMetadata(sourceId, Normal, updatedAt)
  }

  def negate(sourceId: Long, destinationId: Long, position: Long, updatedAt: Time) = {
    write(Seq(Edge(sourceId, destinationId, position, updatedAt, Negative)))
  }

  def negate(sourceId: Long, updatedAt: Time) {
    updateMetadata(sourceId, Negative, updatedAt)
  }

  def remove(sourceId: Long, destinationId: Long, position: Long, updatedAt: Time) = {
    write(Seq(Edge(sourceId, destinationId, position, updatedAt, Removed)))
  }

  def remove(sourceId: Long, updatedAt: Time) {
    updateMetadata(sourceId, Removed, updatedAt)
  }

  def archive(sourceId: Long, destinationId: Long, position: Long, updatedAt: Time) = {
    write(Seq(Edge(sourceId, destinationId, position, updatedAt, Archived)))
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

  private def write(edges: Seq[Edge]) {
    write(edges, config("errors.deadlock_retries").toInt)
  }

  private def incr(column: Int, newColor: Int) = {
    "IF(" + newColor + " = edges.state, 0, " +
      "IF(" + column + " = " + newColor + ", 1, IF(" + column + " = edges.state, -1, 0)))"
  }

  private def state_priority(state: String): String = "-IF(" + state + "=0, 4, " + state + ")"

  private def write(edges: Seq[Edge], tries: Int) {
    try {
      initializeMetadata(edges.map(_.sourceId))
      initializeEdges(edges)
      edges.foreach { edge =>
        val query = "UPDATE " + tablePrefix + "_metadata AS metadata, " + tablePrefix + "_edges AS edges " +
          "SET " +
          "    metadata.count0 = metadata.count0 + " + incr(0, edge.state.id) + "," +
          "    metadata.count1 = metadata.count1 + " + incr(1, edge.state.id) + "," +
          "    metadata.count2 = metadata.count2 + " + incr(2, edge.state.id) + "," +
          "    metadata.count3 = metadata.count3 + " + incr(3, edge.state.id) + "," +
          "    edges.state            = ?, " +
          "    edges.position         = ?, " +
          "    edges.updated_at       = ? " +
          "WHERE (edges.updated_at    < ? OR (edges.updated_at = ? AND " +
          "(" + state_priority("edges.state") + " < " + state_priority(edge.state.id.toString) + ")))" +
          "  AND edges.source_id      = ? " +
          "  AND edges.destination_id = ? " +
          "  AND metadata.source_id   = ? "
        queryEvaluator.execute(query, edge.state.id, edge.position, edge.updatedAt.inSeconds, edge.updatedAt.inSeconds, edge.updatedAt.inSeconds, edge.sourceId, edge.destinationId, edge.sourceId)
      }
    } catch {
      case e: MySQLTransactionRollbackException if (tries > 0) =>
        write(edges, tries - 1)
    }
  }

  def writeCopies(edges: Seq[Edge]) = write(edges)

  @deprecated
  def withLock[A](sourceId: Long)(f: (Shard, Metadata) => A) = {
    atomically(sourceId) { (transaction, metadata) =>
      f(new SqlShard(transaction, shardInfo, weight, children, config), metadata)
    }
  }

  @deprecated
  private def atomically[A](sourceId: Long)(f: (Transaction, Metadata) => A): A = {
    try {
      queryEvaluator.transaction { transaction =>
        transaction.selectOne("SELECT * FROM " + tablePrefix + "_metadata WHERE source_id = ? FOR UPDATE", sourceId) { row =>
          f(transaction, Metadata(sourceId, State(row.getInt("state")), row.getInt("count0"), row.getInt("count1"), row.getInt("count2"), row.getInt("count3"), Time(row.getInt("updated_at").seconds)))
        } getOrElse(throw new MissingMetadataRow)
      }
    } catch {
      case e: MissingMetadataRow =>
        initializeMetadata(sourceId)
        atomically(sourceId)(f)
    }
  }


  def initializeMetadata(sourceId: Long): Unit = initializeMetadata(Seq(sourceId))

  def initializeMetadata(sourceIds: Seq[Long]): Unit = {
    val values = sourceIds.map("(" + _ + ")").mkString(",")
    val query = "INSERT IGNORE INTO " + tablePrefix + "_metadata (source_id) VALUES " + values
    queryEvaluator.execute(query)
  }

  def initializeEdges(edges: Seq[Edge]) = {
    val values = edges.map{ edge => "(" + edge.sourceId + ", " + edge.destinationId + ", 0, "+edge.position+", -1)"}.mkString(",")
    val query = "INSERT IGNORE INTO " + tablePrefix + "_edges (source_id, destination_id, updated_at, position, state) VALUES " + values
    queryEvaluator.execute(query)
  }

  // writeMetadataState(Metadata(sourceId, Normal, 0, Time.now))

  def writeMetadataState(metadatas: Seq[Metadata])  = {
    def update_value_if_newer_or_better(column: String) = column + "=IF(updated_at < VALUES(updated_at) OR (updated_at = VALUES(updated_at) AND "+state_priority("state") + " < "+state_priority("VALUES(state)")+"), VALUES(" + column + "), " + column + ")"

    val query = "INSERT INTO " + tablePrefix + "_metadata " +
                "(source_id, state, updated_at) VALUES " +
                List.make(metadatas.length, "(?, ?, ?)").mkString(", ") +
                " ON DUPLICATE KEY UPDATE " +
                update_value_if_newer_or_better("state") + " , " +
                update_value_if_newer_or_better("updated_at")
    val params = metadatas.foldLeft(List[Any]())((memo, m) => memo ++ List(m.sourceId, m.state.id, m.updatedAt.inSeconds))

    queryEvaluator.execute(query, params: _*)
  }

  def writeMetadataState(metadata: Metadata) = this.writeMetadataState(List(metadata))

  def updateMetadata(metadata: Metadata): Unit = updateMetadata(metadata.sourceId, metadata.state, metadata.updatedAt)

  def updateMetadata(sourceId: Long, state: State, updatedAt: Time) {
    writeMetadataState(Metadata(sourceId, state, 0, updatedAt))
  }

  private def makeEdge(row: ResultSet): Edge = {
    Edge(row.getLong("source_id"), row.getLong("destination_id"), row.getLong("position"), Time(row.getInt("updated_at").seconds), State(row.getInt("state")))
  }
}
