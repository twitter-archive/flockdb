package com.twitter.flockdb

import com.twitter.gizzard.test.Database
import com.twitter.ostrich.W3CStats
import net.lag.configgy.Configgy
import net.lag.logging.Logger


object StaticEdges extends Database {
  Configgy.configure("config/test.conf")
  val poolConfig = Configgy.config.configMap("db.connection_pool")
  val log = Logger.get
  val config = Configgy.config
  val w3c = new W3CStats(log, config.getList("edges.w3c").toArray)
  val stats = Edges.statsCollector(w3c)
  lazy val edges = try {
    Edges(config, databaseFactory, databaseFactory, w3c, stats)
  } catch {
    case e =>
      e.printStackTrace()
      throw e
  }
}
