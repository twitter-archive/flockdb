package com.twitter.flockdb

import net.lag.configgy.Configgy
import com.twitter.service.flock.Database
import com.twitter.ostrich.W3CStats
import net.lag.logging.Logger


object StaticEdges {
  import Database._
  val log = Logger.get
  val edges = Edges(config, new W3CStats(log, config.getList("edges.w3c").toArray), databaseFactory)
}
