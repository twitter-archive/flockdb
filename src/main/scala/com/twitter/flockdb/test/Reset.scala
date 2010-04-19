package com.twitter.flockdb

import com.twitter.querulous.query.SqlQueryFactory
import com.twitter.querulous.evaluator.StandardQueryEvaluatorFactory
import com.twitter.xrayspecs.Time
import com.twitter.xrayspecs.TimeConversions._
import net.lag.configgy.{ConfigMap, Configgy}
import org.specs.Specification


trait Reset {
  import Database._
  val queryEvaluatorFactory = new StandardQueryEvaluatorFactory(databaseFactory, new SqlQueryFactory)

  def reset(config: ConfigMap) {
    config.getConfigMap("ids").foreach { id =>
      val idQueryEvaluator = queryEvaluatorFactory(id("hostname"), id("database"), id("username"), id("password"))
      idQueryEvaluator.execute("DELETE FROM " + id("table"))
      idQueryEvaluator.execute("INSERT INTO " + id("table") + " VALUES (0)")
    }

    val key = config.configMap("nameservers").keys.next
    val ns = config.configMap("nameservers." + key)
    val nameServerQueryEvaluator = queryEvaluatorFactory(ns("hostname"), ns("database"), ns("username"), ns("password"))

    nameServerQueryEvaluator.execute("DELETE FROM forwardings")
    nameServerQueryEvaluator.execute("DELETE FROM shard_children")
    nameServerQueryEvaluator.execute("DELETE FROM shards")
  }

  def reset(database: String) {
    Time.reset()
    Time.freeze()
    val queryEvaluator = queryEvaluatorFactory("localhost", null, config("db.username"), config("db.password"))
    queryEvaluator.execute("DROP DATABASE IF EXISTS " + database)
  }
}
