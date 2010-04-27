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
