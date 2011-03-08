package com.twitter.flockdb.config

import com.twitter.gizzard.config._
import com.twitter.querulous.config.{Connection, QueryEvaluator}
import com.twitter.util.TimeConversions._
import flockdb.queries.Query
import flockdb.queries

trait AdminConfig {
  def httpPort: Int
  def textPort: Int
}

trait FlockDBServer extends TServer {
  var name = "flockdb_edges"
  var port = 7915
}

trait IntersectionQuery {
  var intersectionTimeout           = 100.millis
  var averageIntersectionProportion = 0.1
  var intersectionPageSizeMax       = 4000

  def intersect(query1: queries.Query, query2: queries.Query) = new queries.IntersectionQuery(query1, query2, averageIntersectionProportion, intersectionPageSizeMax, intersectionTimeout)
  def difference(query1: queries.Query, query2: queries.Query) = new queries.DifferenceQuery(query1, query2, averageIntersectionProportion, intersectionPageSizeMax, intersectionTimeout)
}

trait FlockDB extends gizzard.config.GizzardServer {
  def server: FlockDBServer
  var migrationServer: Option[TServer] = None

  var intersectionQuery: IntersectionQuery = new IntersectionQuery { }
  var aggregateJobsPageSize         = 500

  def databaseConnection: Connection
  def edgesQueryEvaluator: QueryEvaluator
  def materializingQueryEvaluator: QueryEvaluator

  def replicationFuture: Future
  def readFuture: Future

  def adminConfig: AdminConfig
}
