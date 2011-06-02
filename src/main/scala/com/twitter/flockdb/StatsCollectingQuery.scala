package com.twitter.flockdb

import com.twitter.gizzard.Stats
import com.twitter.querulous.database.{Database, DatabaseFactory}
import com.twitter.querulous.query.{Query, QueryFactory, QueryClass, QueryProxy}
import com.twitter.util.Duration
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
    val (rv, duration) = Duration.inMilliseconds { f }
    Stats.transaction.record("Query duration: "+duration.inMillis)
    rv
  }
}

class TransactionStatsCollectingDatabaseFactory(databaseFactory: DatabaseFactory) extends DatabaseFactory {
  def apply(dbhosts: List[String], dbname: String, username: String, password: String, urlOptions: Map[String, String]) = {
    new TransactionStatsCollectingDatabase(databaseFactory(dbhosts, dbname, username, password, urlOptions), dbhosts)
  }
}

class TransactionStatsCollectingDatabase(database: Database, dbhosts: List[String]) extends Database {
  override def open(): Connection = {
    Stats.transaction.record("Opening a connection to: "+dbhosts.mkString(","))
    val (rv, duration) = Duration.inMilliseconds { database.open() }
    Stats.transaction.record("Open duration: "+duration.inMillis)
    rv
  }

  override def close(connection: Connection) = {
    Stats.transaction.record("Closing connection to: "+dbhosts.mkString(","))
    val (rv, duration) = Duration.inMilliseconds { database.close(connection) }
    Stats.transaction.record("Close duration: "+duration.inMillis)
  }
}
