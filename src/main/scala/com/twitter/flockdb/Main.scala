/*
 * Copyright 2010 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.twitter.flockdb

import java.util.concurrent.CountDownLatch
import scala.actors.Actor.actor
import org.apache.thrift.TProcessor
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.server.{TServer, TThreadPoolServer}
import org.apache.thrift.transport.{TServerSocket, TTransportFactory}
import com.twitter.gizzard.proxy.{ExceptionHandlingProxy, LoggingProxy}
import com.twitter.gizzard.thrift.{GizzardServices, JobManager, JobManagerService, ShardManager,
  ShardManagerService, TSelectorServer}
import com.twitter.ostrich.{JsonStatsLogger, Service, ServiceTracker, Stats, StatsMBean, W3CStats}
import com.twitter.xrayspecs.TimeConversions._
import net.lag.configgy.{Config, RuntimeEnvironment, ConfigMap, Configgy}
import net.lag.logging.Logger


object Main extends Service {
  private val log = Logger.get(getClass.getName)
  val runtime = new RuntimeEnvironment(getClass)

  var thriftServer: TSelectorServer = null
  var gizzardServices: GizzardServices[shards.Shard] = null
  var flock: FlockDB = null

  var config: ConfigMap = null

  private val deathSwitch = new CountDownLatch(1)
  private val serverLatch = new CountDownLatch(1)
  var statsLogger: JsonStatsLogger = null

  object FlockExceptionWrappingProxy extends ExceptionHandlingProxy({ e =>
    e match {
      case _: thrift.FlockException =>
        throw e
      case _ =>
        log.error(e, "Error in FlockDB: " + e)
        throw new thrift.FlockException(e.toString)
    }
  })

  def main(args: Array[String]) {
    runtime.load(args)
    config = Configgy.config

    val maxThreads = config.getInt("max_threads", Runtime.getRuntime().availableProcessors * 2)
    System.setProperty("actors.maxPoolSize", maxThreads.toString)
    System.setProperty("actors.corePoolSize", "1")
    log.debug("max_threads=%d", maxThreads)

    StatsMBean("com.twitter.flockdb")

    // workaround for actor lifetime issue:
    actor { deathSwitch.await }

    startAdmin()
    startThrift()

    statsLogger = new JsonStatsLogger(Logger.get("stats"), 1.minute)
    statsLogger.start()
  }

  def shutdown() {
    log.info("Shutting down.")
    stopThrift()
    finishShutdown()
  }

  def quiesce() {
    log.info("Going quiescent.")
    stopThrift()

    while (flock.edges.schedule.size > 0) {
      log.info("Waiting for job queue to drain: jobs=%d", flock.edges.schedule.size)
      Thread.sleep(100)
    }

    log.info("Shutting down.")
    finishShutdown()
  }

  def startAdmin() = {
    ServiceTracker.register(this)
    ServiceTracker.startAdmin(config, runtime)
  }

  def startThrift() {
    try {
      val w3c = new W3CStats(Logger.get("w3c"), config.getList("edges.w3c").toArray)

      // there are a bunch of things we report that aren't in the w3c list above.
      w3c.complainAboutUnregisteredFields = false

      flock = FlockDB(config, w3c)

      val processor = new thrift.FlockDB.Processor(
        FlockExceptionWrappingProxy[thrift.FlockDB.Iface](
          LoggingProxy[thrift.FlockDB.Iface](Stats, w3c, "Edges", flock)))
      thriftServer = TSelectorServer("edges", config("edges.server_port").toInt,
                                     config.configMap("gizzard_services"), processor)
      gizzardServices = new GizzardServices(config.configMap("gizzard_services"),
                                            flock.edges.nameServer,
                                            flock.edges.copyFactory,
                                            flock.edges.schedule,
                                            Priority.Medium.id)
      gizzardServices.start()
      thriftServer.serve()
    } catch {
      case e: Exception =>
        log.error(e, "Unexpected exception: %s", e.getMessage)
        System.exit(0)
    }
  }

  def stopThrift() {
    // thrift bug doesn't let worker threads know to close their connection, so we wait for any
    // pending requests to finish.
    log.info("Thrift servers shutting down...")
    thriftServer.shutdown()
    thriftServer = null
    gizzardServices.shutdown()
    gizzardServices = null
  }

  def finishShutdown() {
    flock.edges.shutdown()
    if (statsLogger ne null) {
      statsLogger.shutdown()
      statsLogger = null
    }
    deathSwitch.countDown()
    log.info("Goodbye!")
  }
}
