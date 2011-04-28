import com.twitter.flockdb.config._
import com.twitter.gizzard.config._
import com.twitter.querulous.config._
import com.twitter.querulous.StatsCollector
import com.twitter.conversions.time._
import com.twitter.conversions.storage._
import com.twitter.flockdb.shards.QueryClass
import com.twitter.flockdb.Priority
import com.twitter.ostrich.admin.config.AdminServiceConfig
import com.twitter.logging.Level
import com.twitter.logging.config._

trait Credentials extends Connection {
  val username = "root"
  val password = ""
}

class ProductionQueryEvaluator extends QueryEvaluator {
  autoDisable = new AutoDisablingQueryEvaluator {
    val errorCount = 100
    val interval = 60.seconds
  }

  database.memoize = true
  database.pool = new ApachePoolingDatabase {
    sizeMin = 40
    sizeMax = 40
    maxWait = 100.millis
    minEvictableIdle = 60.seconds
    testIdle = 1.second
    testOnBorrow = false
  }

  database.timeout = new TimingOutDatabase {
    open = 50.millis
    poolSize = 10
    queueSize = 10000
  }
}

class ProductionNameServerReplica(host: String) extends Mysql {
  val connection = new Connection with Credentials {
    val hostnames = Seq(host)
    val database = "flock_edges_production"
  }

  queryEvaluator = new ProductionQueryEvaluator {
    database.pool.foreach { p =>
      p.sizeMin = 1
      p.sizeMax = 1
      p.maxWait = 1.second
    }

    database.timeout.foreach { t =>
      t.open = 1.second
    }

    query.timeouts = Map(
      QueryClass.Select -> QueryTimeout(1.second),
      QueryClass.Execute -> QueryTimeout(1.second),
      QueryClass.SelectCopy -> QueryTimeout(15.seconds),
      QueryClass.SelectModify -> QueryTimeout(3.seconds),
      QueryClass.SelectSmall                -> QueryTimeout(1.second),
      QueryClass.SelectIntersection         -> QueryTimeout(1.second),
      QueryClass.SelectMetadata             -> QueryTimeout(1.second),
      QueryClass.SelectMetadataIntersection -> QueryTimeout(1.second)
    )
  }
}

new FlockDB {
  val server = new FlockDBServer with THsHaServer {
    timeout = 100.millis
    idleTimeout = 60.seconds
    threadPool.minThreads = 250
    threadPool.maxThreads = 250
  }

  val nameServer = new com.twitter.gizzard.config.NameServer {
    mappingFunction = ByteSwapper
    jobRelay = NoJobRelay

    val replicas = Seq(
      new ProductionNameServerReplica("flockdb001.twitter.com"),
      new ProductionNameServerReplica("flockdb002.twitter.com")
    )
  }

  jobInjector.timeout = 100.millis
  jobInjector.idleTimeout = 60.seconds
  jobInjector.threadPool.minThreads = 30

  val replicationFuture = new Future {
    poolSize = 100
    maxPoolSize = 100
    keepAlive = 5.seconds
    timeout = 6.seconds
  }

  val readFuture = new Future {
    poolSize = 100
    maxPoolSize = 100
    keepAlive = 5.seconds
    timeout = 6.seconds
  }

  val databaseConnection = new Credentials {
    val hostnames = Seq("localhost")
    val database = "edges"
    urlOptions = Map("rewriteBatchedStatements" -> "true")
  }

  val edgesQueryEvaluator = new ProductionQueryEvaluator

  val materializingQueryEvaluator = new ProductionQueryEvaluator {
    database.pool.foreach { p =>
      p.sizeMin = 1
      p.sizeMax = 1
      p.maxWait = 1.second
    }
  }

  class ProductionScheduler(val name: String) extends Scheduler {
    jobQueueName = name + "_jobs"

    val schedulerType = new KestrelScheduler {
      path = "/var/spool/kestrel"
      maxMemorySize = 36.megabytes
    }

    errorLimit = 100
    errorRetryDelay = 15.minutes
    errorStrobeInterval = 1.second
    perFlushItemLimit = 100
    jitterRate = 0
  }

  val jobQueues = Map(
    Priority.High.id    -> new ProductionScheduler("edges") { threads = 32 },
    Priority.Medium.id  -> new ProductionScheduler("copy") { threads = 12; errorRetryDelay = 60.seconds },
    Priority.Low.id     -> new ProductionScheduler("edges_slow") { threads = 2 }
  )

  val adminConfig = new AdminServiceConfig {
    httpPort = Some(9990)
  }

  loggers = List(
    new LoggerConfig {
      level = Some(Level.INFO)
      handlers = List(
        new ThrottledHandlerConfig {
          duration = 60.seconds
          maxToDisplay = 10
          handler = new FileHandlerConfig {
            filename = "/var/log/flock/production.log"
            roll = Policy.Hourly
          }
        })
    },
    new LoggerConfig {
      node = "stats"
      useParents = false
      level = Some(Level.INFO)
      handlers = List(new ScribeHandlerConfig {
        category = "flock-stats"
      })
    },
    new LoggerConfig {
      node = "bad_jobs"
      useParents = false
      level = Some(Level.INFO)
      handlers = List(new FileHandlerConfig {
        roll = Policy.Never
        filename = "/var/log/flock/bad_jobs.log"
      })
    })
}
