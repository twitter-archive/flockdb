package com.twitter.service.flock

import com.twitter.gizzard.nameserver.NameServer
import com.twitter.querulous.database.ApachePoolingDatabaseFactory
import com.twitter.querulous.evaluator.{QueryEvaluator, StandardQueryEvaluatorFactory}
import com.twitter.querulous.query.SqlQueryFactory
import com.twitter.xrayspecs.TimeConversions._
import com.twitter.xrayspecs.XraySpecsRunner
import net.lag.configgy.Configgy


object TestRunner extends XraySpecsRunner("src/test/scala/**/" +
  System.getProperty("test_phase", "(unit|integration)") + "/*.scala")
