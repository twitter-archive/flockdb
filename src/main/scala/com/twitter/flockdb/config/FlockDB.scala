package com.twitter.flockdb.config

import com.twitter.gizzard.config._
import com.twitter.ostrich.admin.config.AdminServiceConfig
import com.twitter.querulous.config.{Connection, AsyncQueryEvaluator}
import com.twitter.util.TimeConversions._
import com.twitter.flockdb.queries.QueryTree
import com.twitter.flockdb.queries


class FlockDBServer {
  var name = "flockdb_edges"
  var port = 7915
  var maxConcurrentRequests = 10000
}

trait IntersectionQuery {
  var intersectionTimeout           = 100.millis
  var averageIntersectionProportion = 0.1
  var intersectionPageSizeMax       = 4000

  def intersect(query1: QueryTree, query2: QueryTree) = new queries.IntersectionQuery(query1, query2, averageIntersectionProportion, intersectionPageSizeMax, intersectionTimeout)
  def difference(query1: QueryTree, query2: QueryTree) = new queries.DifferenceQuery(query1, query2, averageIntersectionProportion, intersectionPageSizeMax, intersectionTimeout)
}

trait FlockDB extends GizzardServer {
  var server = new FlockDBServer

  var intersectionQuery: IntersectionQuery = new IntersectionQuery { }
  var aggregateJobsPageSize         = 500

  def databaseConnection: Connection

  def edgesQueryEvaluator: AsyncQueryEvaluator
  def lowLatencyQueryEvaluator: AsyncQueryEvaluator
  def materializingQueryEvaluator: AsyncQueryEvaluator

  def adminConfig: AdminServiceConfig
}
