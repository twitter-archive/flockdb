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

class ProductionQueryEvaluator extends AsyncQueryEvaluator {
  override var workPoolSize = 40
  database.memoize = true
  database.pool = new ThrottledPoolingDatabase {
    size = workPoolSize
    openTimeout = 100.millis
  }

  query.timeouts = Map(
    QueryClass.Select                  -> QueryTimeout(1.second),
    QueryClass.Execute                 -> QueryTimeout(1.second),
    QueryClass.SelectCopy              -> QueryTimeout(15.seconds),
    QueryClass.SelectModify            -> QueryTimeout(3.seconds),
    QueryClass.SelectSingle            -> QueryTimeout(1.second),
    QueryClass.SelectIntersection      -> QueryTimeout(1.second),
    QueryClass.SelectIntersectionSmall -> QueryTimeout(1.second),
    QueryClass.SelectMetadata          -> QueryTimeout(1.second)
  )
}

class ProductionNameServerReplica(host: String) extends Mysql {
  val connection = new Connection with Credentials {
    val hostnames = Seq(host)
    val database = "flock_edges_production"
  }

  queryEvaluator = new QueryEvaluator {
    database.memoize = true
    database.pool = new ThrottledPoolingDatabase {
      size = 1
      openTimeout = 1.second
    }
  }
}

new FlockDB {
  mappingFunction = ByteSwapper
  jobRelay        = NoJobRelay

  nameServerReplicas = Seq(
    new ProductionNameServerReplica("flockdb001.twitter.com"),
    new ProductionNameServerReplica("flockdb002.twitter.com")
  )

  jobInjector.timeout               = 100.millis
  jobInjector.idleTimeout           = 60.seconds
  jobInjector.threadPool.minThreads = 30

  val databaseConnection = new Credentials {
    val hostnames = Seq("localhost")
    val database = "edges"
    urlOptions = Map("rewriteBatchedStatements" -> "true")
  }

  val edgesQueryEvaluator = new ProductionQueryEvaluator
  val lowLatencyQueryEvaluator = new ProductionQueryEvaluator

  val materializingQueryEvaluator = new ProductionQueryEvaluator {
    workPoolSize = 1
    database.pool = new ThrottledPoolingDatabase {
      size = workPoolSize
      openTimeout = 1.second
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
