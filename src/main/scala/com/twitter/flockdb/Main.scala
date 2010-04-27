package com.twitter.flockdb

import java.util.concurrent.{CountDownLatch}
import scala.actors.Actor.actor
import org.apache.thrift.TProcessor
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.server.{TServer, TThreadPoolServer}
import org.apache.thrift.transport.{TServerSocket, TTransportFactory}
import com.twitter.gizzard.proxy.{ExceptionHandlingProxy, LoggingProxy}
import com.twitter.gizzard.thrift.{JobManager, JobManagerService, ShardManager,
  ShardManagerService, TSelectorServer}
import com.twitter.ostrich.{JsonStatsLogger, Service, ServiceTracker, Stats, StatsMBean, W3CStats}
import com.twitter.xrayspecs.TimeConversions._
import net.lag.configgy.{Config, RuntimeEnvironment, ConfigMap, Configgy}
import net.lag.logging.Logger


object Main extends Service {
  private val log = Logger.get(getClass.getName)
  val runtime = new RuntimeEnvironment(getClass)

  var thriftServer: TSelectorServer = null
  var shardThriftServer: TSelectorServer = null
  var jobThriftServer: TSelectorServer = null
  var flock: FlockDB = null

  var config: ConfigMap = null

  private val deathSwitch = new CountDownLatch(1)
  private val serverLatch = new CountDownLatch(1)
  var statsLogger: JsonStatsLogger = null

  object FlockExceptionWrappingProxy extends ExceptionHandlingProxy({ e =>
    log.error(e, "Error in FlockDB: " + e)
    throw new thrift.FlockException(e.toString)
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

    while (flock.schedule.size > 0) {
      log.info("Waiting for job queue to drain: jobs=%d", flock.schedule.size)
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

      val clientTimeout = config("edges.client_timeout_msec").toInt.milliseconds
      val idleTimeout = config("edges.idle_timeout_sec").toInt.seconds

      val executor = TSelectorServer.makeThreadPoolExecutor(config.configMap("edges"))

      val processor = new thrift.FlockDB.Processor(
        FlockExceptionWrappingProxy[thrift.FlockDB.Iface](
          LoggingProxy[thrift.FlockDB.Iface](Stats, w3c, "Edges", flock)))
      thriftServer = TSelectorServer("edges", config("edges.server_port").toInt, processor,
                                     executor, clientTimeout, idleTimeout)

      val shardServer = new ShardManagerService(flock.nameServer, flock.copyFactory,
                                                flock.schedule(Priority.Medium.id))
      val shardProcessor = new ShardManager.Processor(
        FlockExceptionWrappingProxy[ShardManager.Iface](
          LoggingProxy[ShardManager.Iface](Stats, w3c, "EdgesShards", shardServer)))
      shardThriftServer = TSelectorServer("edges-shards", config("edges.shard_server_port").toInt,
                                          shardProcessor, executor, clientTimeout, idleTimeout)
      val jobServer = new JobManagerService(flock.schedule)
      val jobProcessor = new JobManager.Processor(
        FlockExceptionWrappingProxy[JobManager.Iface](
          LoggingProxy[JobManager.Iface](Stats, w3c, "EdgesJobs", jobServer)))
      jobThriftServer = TSelectorServer("edges-jobs", config("edges.job_server_port").toInt,
                                        jobProcessor, executor, clientTimeout, idleTimeout)

      thriftServer.serve()
      shardThriftServer.serve()
      jobThriftServer.serve()
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
    thriftServer.stop()
    thriftServer = null
    shardThriftServer.stop()
    shardThriftServer = null
    jobThriftServer.stop()
    jobThriftServer = null
  }

  def finishShutdown() {
    flock.schedule.shutdown()
    if (statsLogger ne null) {
      statsLogger.shutdown()
      statsLogger = null
    }
    deathSwitch.countDown()
    log.info("Goodbye!")
  }
}
