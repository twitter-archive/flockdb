package com.twitter.flockdb.config

import com.twitter.gizzard.config._
import com.twitter.ostrich.admin.config.AdminServiceConfig
import com.twitter.querulous.config.{Connection, QueryEvaluator}
import com.twitter.util.TimeConversions._
import com.twitter.flockdb.queries.QueryTree
import com.twitter.flockdb.queries
import java.net.InetSocketAddress

trait FlockDBServer extends TServer {
  var name = "flockdb_edges"
  var port = 7915
}

trait IntersectionQuery {
  var intersectionTimeout           = 100.millis
  var averageIntersectionProportion = 0.1
  var intersectionPageSizeMax       = 4000

  def intersect(query1: QueryTree, query2: QueryTree) = new queries.IntersectionQuery(query1, query2, averageIntersectionProportion, intersectionPageSizeMax, intersectionTimeout)
  def difference(query1: QueryTree, query2: QueryTree) = new queries.DifferenceQuery(query1, query2, averageIntersectionProportion, intersectionPageSizeMax, intersectionTimeout)
}

trait FlockDB extends GizzardServer {
  def server: FlockDBServer

  var intersectionQuery: IntersectionQuery = new IntersectionQuery { }
  var aggregateJobsPageSize         = 500

  def databaseConnection: Connection

  def edgesQueryEvaluator: QueryEvaluator
  def lowLatencyQueryEvaluator: QueryEvaluator
  def materializingQueryEvaluator: QueryEvaluator

  def readFuture: Future

  def adminConfig: AdminServiceConfig

  case class ZooKeeperSettings(server: String, basePath: String)
  def zooKeeperSettings: Option[ZooKeeperSettings] = None
}
