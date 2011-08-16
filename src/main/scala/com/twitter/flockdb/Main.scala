package com.twitter.flockdb

import com.twitter.util.Eval
import com.twitter.logging.Logger
import com.twitter.ostrich.admin.{Service, ServiceTracker, RuntimeEnvironment, AdminHttpService}
import java.io.File

import com.twitter.flockdb.config.{FlockDB => FlockDBConfig}

object Main {
  val log = Logger.get

  var adminServer: Option[AdminHttpService] = None

  def main(args: Array[String]) {
    try {
      log.info("Starting FlockDB.")

      val eval    = new Eval
      val config  = eval[FlockDBConfig](args.map(new File(_)): _*)
      val runtime = new RuntimeEnvironment(this)

      Logger.configure(config.loggers)
      adminServer = config.adminConfig()(runtime)

      val service = new FlockDB(config)

      ServiceTracker.register(service)
      service.start()

    } catch {
      case e => {
        log.fatal(e, "Exception in initialization: ", e.getMessage)
        log.fatal(e.getStackTrace.toString)
        System.exit(1)
      }
    }
  }
}
