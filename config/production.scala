import com.twitter.flockdb.config._
import com.twitter.gizzard.config._
import com.twitter.querulous.config._
import com.twitter.querulous.StatsCollector
import com.twitter.conversions.time._
import com.twitter.conversions.storage._
import com.twitter.flockdb.shards.QueryClass
import com.twitter.flockdb.Priority

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
  database.pool = new ThrottledPoolingDatabase {
    size = 40
    openTimeout = 100.millis
  }
}

class ProductionNameServerReplica(host: String) extends Mysql {
  val connection = new Connection with Credentials {
    val hostnames = Seq(host)
    val database = "flock_edges_production"
  }

  queryEvaluator = new ProductionQueryEvaluator {
    database.pool = new ThrottledPoolingDatabase {
      size = 1
      openTimeout = 1.second
    }

    query.timeouts = Map(
      QueryClass.Select -> QueryTimeout(1.second),
      QueryClass.Execute -> QueryTimeout(1.second),
      QueryClass.SelectCopy -> QueryTimeout(15.seconds),
      QueryClass.SelectModify -> QueryTimeout(3.seconds)
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
    database.pool = new ThrottledPoolingDatabase {
      size = 1
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

  val adminConfig = new AdminConfig {
    val textPort = 9991
    val httpPort = 9990
  }

  logging = new LogConfigString("""
log {
  filename = "/var/log/flock/production.log"
  level = "info"
  roll = "hourly"
  throttle_period_msec = 60000
  throttle_rate = 10
  truncate_stack_traces = 100

  w3c {
    node = "w3c"
    use_parents = false
    filename = "/var/log/flock/w3c.log"
    level = "info"
    roll = "hourly"
  }

  stats {
    node = "stats"
    use_parents = false
    level = "info"
    scribe_category = "flock-stats"
    scribe_server = "localhost"
    scribe_max_packet_size = 100
  }

  bad_jobs {
    node = "bad_jobs"
    use_parents = false
    filename = "/var/log/flock/bad_jobs.log"
    level = "info"
    roll = "never"
  }
}
  """)
}
