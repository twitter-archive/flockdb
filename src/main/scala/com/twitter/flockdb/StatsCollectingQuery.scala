package com.twitter.flockdb

import com.twitter.gizzard.Stats
import com.twitter.querulous.database.{Database, DatabaseFactory, DatabaseProxy}
import com.twitter.querulous.query.{Query, QueryFactory, QueryClass, QueryProxy}
import com.twitter.util.{Time, Duration}
import java.sql.Connection

class TransactionStatsCollectingQueryFactory(queryFactory: QueryFactory)
  extends QueryFactory {

  def apply(connection: Connection, queryClass: QueryClass, query: String, params: Any*) = {
    new TransactionStatsCollectingQuery(queryFactory(connection, queryClass, query, params: _*), queryClass, query)
  }
}

class TransactionStatsCollectingQuery(query: Query, queryClass: QueryClass, queryString: String) extends QueryProxy(query) {
  override def delegate[A](f: => A) = {
    Stats.transaction.record("Executing "+queryClass.name+" query: "+queryString)
    val start = Time.now
    try {
      val rv = f
      val duration = Time.now - start
      Stats.transaction.record("Query duration: "+duration.inMillis)
      rv
    } catch {
      case e =>
        Stats.transaction.record("Failure executing query: "+e)
        val duration = Time.now - start
        Stats.transaction.record("Query duration: "+duration.inMillis)
        throw e
    }
  }
}

class TransactionStatsCollectingDatabaseFactory(databaseFactory: DatabaseFactory) extends DatabaseFactory {
  def apply(dbhosts: List[String], dbname: String, username: String, password: String, urlOptions: Map[String, String], driverName: String) = {
    new TransactionStatsCollectingDatabase(databaseFactory(dbhosts, dbname, username, password, urlOptions, driverName), dbhosts)
  }
}

class TransactionStatsCollectingDatabase(val database: Database, dbhosts: List[String]) extends DatabaseProxy {
  override def open(): Connection = {
    Stats.transaction.record("Opening a connection to: "+dbhosts.mkString(","))
    val start = Time.now
    try {
      val rv = database.open()
      val duration = Time.now-start
      Stats.transaction.record("Open duration: "+duration.inMillis)
      rv
    } catch {
      case e =>
        Stats.transaction.record("Failure opening a connection: "+e)
        val duration = Time.now-start
        Stats.transaction.record("Open duration: "+duration.inMillis)
        throw e
    }
  }

  override def close(connection: Connection) = {
    Stats.transaction.record("Closing connection to: "+dbhosts.mkString(","))
    val start = Time.now
    try {
      val rv = database.close(connection)
      val duration = Time.now - start
      Stats.transaction.record("Close duration: "+duration.inMillis)
      rv
    } catch {
      case e =>
        Stats.transaction.record("Failure closing a connection: "+e)
        val duration = Time.now-start
        Stats.transaction.record("Close duration: "+duration.inMillis)
        throw e
    }
  }
}
