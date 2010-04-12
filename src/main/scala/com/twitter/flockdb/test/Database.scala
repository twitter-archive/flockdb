package com.twitter.service.flock

import net.lag.configgy.Configgy
import com.twitter.querulous.query.SqlQueryFactory
import com.twitter.querulous.database.{ApachePoolingDatabaseFactory, MemoizingDatabaseFactory}
import com.twitter.querulous.evaluator.StandardQueryEvaluatorFactory
import com.twitter.xrayspecs.Time
import com.twitter.xrayspecs.TimeConversions._

object Database {
  val config = Configgy.config
  val databaseFactory = new MemoizingDatabaseFactory(new ApachePoolingDatabaseFactory(
    config("db.connection_pool.size_min").toInt,
    config("db.connection_pool.size_max").toInt,
    config("db.connection_pool.test_idle_msec").toLong.millis,
    config("db.connection_pool.max_wait").toLong.millis,
    config("db.connection_pool.test_on_borrow").toBoolean,
    config("db.connection_pool.min_evictable_idle_msec").toLong.millis))
}

