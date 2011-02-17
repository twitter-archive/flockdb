package com.twitter.flockdb

import com.twitter.ostrich.{W3CStats, Stats, Service, ServiceTracker, W3CReporter, RuntimeEnvironment}
import com.twitter.util.Eval
import net.lag.configgy.{Config => CConfig, Configgy}
import net.lag.logging.{FileHandler, Logger}
import org.apache.thrift.server.TServer
import java.io.File
import config.{FlockDB => FlockDBConfig}

object Main extends Service {
  var service: FlockDB = null
  var config: FlockDBConfig = null

  val w3cItems = Array(
    "second",
    "minute",
    "hour",
    "timestamp",
    "action-timing",
    "result-count",
    "db-timing",
    "db-open-timing",
    "db-open-timeout-count",
    "db-close-timeout-count",
    "db-close-timing",
    "connection-pool-release-timing",
    "connection-pool-reserve-timing",
    "kestrel-put-timing",
    "db-select-count",
    "db-select-timing",
    "db-select_modify-count",
    "db-select_modify-timing",
    "db-execute-count",
    "db-execute-timing",
    "db-query-count-default",
    "db-query-timeout-count",
    "db-query-select-timeout-count",
    "db-query-select_execute-timeout-count",
    "db-query-select_modify-timeout-count",
    "x-db-query-count-default",
    "x-db-query-timing-default",
    "job-success-count",
    "operation",
    "arguments"
  )

  lazy val w3c = new W3CStats(Logger.get("w3c"), w3cItems)

  def main(args: Array[String]) {
    try {
      config  = Eval[FlockDBConfig](args.map(new File(_)): _*)
      service = new FlockDB(config, w3c)
//      Configgy.configLogging(CConfig.fromString(config.loggingConfig))

      start()

      println("Running.")
    } catch {
      case e => {
        println("Exception in initialization: ")
        Logger.get("").fatal(e, "Exception in initialization.")
        e.printStackTrace
        shutdown()
      }
    }
  }

  def start() {
    ServiceTracker.register(this)
    val adminConfig = new CConfig
    adminConfig.setInt("admin_http_port", config.adminConfig.httpPort)
    adminConfig.setInt("admin_text_port", config.adminConfig.textPort)
    ServiceTracker.startAdmin(adminConfig, new RuntimeEnvironment(this.getClass))

    service.start()
  }

  def shutdown() {
    if (service ne null) service.shutdown()
    service = null
    ServiceTracker.stopAdmin()
  }

  def quiesce() {
    if (service ne null) service.shutdown(true)
    service = null
    ServiceTracker.stopAdmin()
  }
}
