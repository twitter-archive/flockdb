package com.twitter.flockdb

import com.twitter.querulous.StatsCollector
import com.twitter.querulous.database.{ApachePoolingDatabaseFactory, DatabaseFactory, MemoizingDatabaseFactory, StatsCollectingDatabaseFactory, TimingOutDatabaseFactory}
import com.twitter.querulous.evaluator.{AutoDisablingQueryEvaluatorFactory, StandardQueryEvaluatorFactory}
import com.twitter.querulous.query.{SqlQueryFactory, TimingOutQueryFactory, TimingOutStatsCollectingQueryFactory}
import com.twitter.xrayspecs.TimeConversions._
import net.lag.configgy.ConfigMap


object AutoDatabaseFactory {
  def apply(config: ConfigMap, stats: Option[StatsCollector]) = {
    var rv: DatabaseFactory = new ApachePoolingDatabaseFactory(
      config("size_min").toInt,
      config("size_max").toInt,
      config("test_idle_msec").toLong.millis,
      config("max_wait").toLong.millis,
      config("test_on_borrow").toBoolean,
      config("min_evictable_idle_msec").toLong.millis)
    rv = stats match {
      case None => rv
      case Some(s) => new StatsCollectingDatabaseFactory(rv, s)
    }
    rv = new TimingOutDatabaseFactory(rv,
      config("timeout.pool_size").toInt,
      config("timeout.queue_size").toInt,
      config("timeout.open").toLong.millis,
      config("timeout.initialize").toLong.millis,
      config("size_max").toInt)
    new MemoizingDatabaseFactory(rv)
  }
}
