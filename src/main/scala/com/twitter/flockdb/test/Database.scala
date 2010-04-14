package com.twitter.flockdb

import net.lag.configgy.Configgy
import com.twitter.querulous.query.SqlQueryFactory
import com.twitter.querulous.database.{ApachePoolingDatabaseFactory, MemoizingDatabaseFactory}
import com.twitter.querulous.evaluator.StandardQueryEvaluatorFactory
import com.twitter.xrayspecs.Time
import com.twitter.xrayspecs.TimeConversions._


object Database {
  val config = Configgy.config
  val databaseFactory = AutoDatabaseFactory(config.configMap("db.connection_pool"), None)
}
