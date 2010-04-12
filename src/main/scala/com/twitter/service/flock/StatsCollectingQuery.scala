package com.twitter.service.flock

import java.sql.{Connection, ResultSet}
import scala.util.matching.Regex
import com.twitter.xrayspecs.Duration
import com.twitter.querulous.StatsCollector
import com.twitter.querulous.query.{QueryFactory, Query, QueryProxy, TimingOutQuery}
import net.lag.extensions._


object TimingOutStatsCollectingQueryFactory {
  val TABLE_NAME = """(FROM|UPDATE|INSERT INTO|LIMIT)\s+[\w-]+""".r
  val DDL_QUERY = """^\s*((CREATE|DROP|ALTER)\s+(TABLE|DATABASE)|DESCRIBE)\s+""".r

  def simplifiedQuery(query: String) = {
    if (DDL_QUERY.findFirstMatchIn(query).isDefined) {
      "default"
    } else {
      query.regexSub(TABLE_NAME) { m => m.group(1) + "?" }
    }
  }
}

class TimingOutStatsCollectingQueryFactory(queryFactory: QueryFactory, queryInfo: scala.collection.Map[String, (String, Duration)], defaultTimeout: Duration, stats: StatsCollector)
  extends QueryFactory {
  def apply(connection: Connection, query: String, params: Any*) = {
    val simplifiedQueryString = TimingOutStatsCollectingQueryFactory.simplifiedQuery(query)
    val (name, timeout) = queryInfo.getOrElse(simplifiedQueryString, ("default", defaultTimeout))
    new StatsCollectingQuery(new TimingOutQuery(queryFactory(connection, query, params: _*), timeout), name, stats)
  }
}

class StatsCollectingQuery(query: Query, queryName: String, stats: StatsCollector) extends QueryProxy(query) {
  override def select[A](f: ResultSet => A) = {
    stats.incr("db-count-select", 1)
    stats.time("db-timing-select")(delegate(query.select(f)))
  }

  override def execute() = {
    stats.incr("db-count-execute", 1)
    stats.time("db-timing-execute")(delegate(query.execute()))
  }

  override def delegate[A](f: => A) = {
    stats.incr("x-db-count-query-" + queryName, 1)
    stats.time("db-timing") {
      stats.time("x-db-timing-query-" + queryName) {
        f
      }
    }
  }
}
