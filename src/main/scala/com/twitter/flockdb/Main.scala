package com.twitter.flockdb

import com.twitter.ostrich.admin.{ServiceTracker, Service}
import com.twitter.logging.{FileHandler, Logger}
import com.twitter.util.Eval
import org.apache.thrift.server.TServer
import java.io.File
import config.{FlockDB => FlockDBConfig}

object Main extends Service {
  var service: FlockDB = null
  var config: FlockDBConfig = null

  def main(args: Array[String]) {
    try {
      val eval = new Eval
      config  = eval[FlockDBConfig](args.map(new File(_)): _*)
      service = new FlockDB(config)

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
    config.adminConfig()
    service.start()
  }

  def shutdown() {
    if (service ne null) service.shutdown()
    service = null
    ServiceTracker.shutdown()
  }

  override def quiesce() {
    if (service ne null) service.shutdown(true)
    service = null
    ServiceTracker.shutdown()
  }
}
