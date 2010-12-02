package com.twitter.flockdb.config

import com.twitter.gizzard.config._
import com.twitter.querulous.config.{Connection, QueryEvaluator}

trait AdminConfig {
  def httpPort: Int
  def textPort: Int
}

trait FlockDB extends gizzard.config.GizzardServer {
  def server: TServer

  def databaseConnection: Connection
  def edgesQueryEvaluator: QueryEvaluator
  def materializingQueryEvaluator: QueryEvaluator

  def replicationFuture: Future
  def readFuture: Future

  def loggingConfig: String
  def adminConfig: AdminConfig
}
