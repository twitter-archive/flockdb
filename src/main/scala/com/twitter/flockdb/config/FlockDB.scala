package com.twitter.flockdb.config

import com.twitter.gizzard.config._
import com.twitter.querulous.config.{Connection, QueryEvaluator}
import com.twitter.util.TimeConversions._

trait AdminConfig {
  def httpPort: Int
  def textPort: Int
}

trait FlockDBServer extends TServer {
  var name = "flockdb_edges"
  var port = 7915
}

trait FlockDB extends gizzard.config.GizzardServer {
  def server: FlockDBServer

  var intersectionTimeout           = 100.millis
  var averageIntersectionProportion = 0.1
  var intersectionPageSizeMax       = 4000
  var aggregateJobsPageSize         = 500

  def databaseConnection: Connection
  def edgesQueryEvaluator: QueryEvaluator
  def materializingQueryEvaluator: QueryEvaluator

  def replicationFuture: Future
  def readFuture: Future

  def adminConfig: AdminConfig
}
