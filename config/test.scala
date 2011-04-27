import com.twitter.flockdb.config._
import com.twitter.gizzard.config._
import com.twitter.querulous.config._
import com.twitter.querulous.database.DatabaseFactory
import com.twitter.querulous.query.QueryFactory
import com.twitter.querulous.StatsCollector
import com.twitter.conversions.time._
import com.twitter.conversions.storage._
import com.twitter.flockdb.shards.QueryClass
import com.twitter.flockdb.{MemoizedQueryEvaluators, Priority}
import com.twitter.ostrich.admin.config.AdminServiceConfig
import com.twitter.logging.Level
import com.twitter.logging.config.{FileHandlerConfig, LoggerConfig}


trait Credentials extends Connection {
  import scala.collection.JavaConversions._
  val env = System.getenv().toMap
  val username = env.get("DB_USERNAME").getOrElse("root")
  val password = env.get("DB_PASSWORD").getOrElse("")
}

class TestQueryEvaluator(label: String) extends QueryEvaluator {
  autoDisable = new AutoDisablingQueryEvaluator {
    val errorCount = 100
    val interval = 60.seconds
  }

//  query.debug = DebugLog
  database.memoize = true
  database.pool = new ApachePoolingDatabase {
    sizeMin = 8
    sizeMax = 8
    maxWait = 1.second
    minEvictableIdle = 60.seconds
    testIdle = 1.second
    testOnBorrow = false
  }

  database.timeout = new TimingOutDatabase {
    poolSize = 10
    queueSize = 10000
    open = 1.second
  }

  override def apply(stats: StatsCollector, dbStatsFactory: Option[DatabaseFactory => DatabaseFactory], queryStatsFactory: Option[QueryFactory => QueryFactory]) = {
    MemoizedQueryEvaluators.evaluators.getOrElseUpdate(label, { super.apply(stats, dbStatsFactory, queryStatsFactory) } )
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
    mappingFunction = Identity
    jobRelay = NoJobRelay

    val replicas = Seq(new Mysql {
      queryEvaluator  = new TestQueryEvaluator("nameserver")

      val connection = new Connection with Credentials {
        val hostnames = Seq("localhost")
        val database = "flock_edges_test"
      }
    })
  }

  jobInjector.timeout = 100.milliseconds
  jobInjector.idleTimeout = 60.seconds
  jobInjector.threadPool.minThreads = 30

  // futures

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
    timeout = 500.millis
  }


  // Database Connectivity

  val databaseConnection = new Credentials {
    val hostnames = Seq("localhost")
    val database = "edges_test"
    urlOptions = Map("rewriteBatchedStatements" -> "true")
  }

  val edgesQueryEvaluator = new TestQueryEvaluator("edges") {
    query.timeouts = Map(
      QueryClass.Select       -> QueryTimeout(100.millis),
      QueryClass.SelectModify -> QueryTimeout(5.seconds),
      QueryClass.SelectCopy   -> QueryTimeout(15.seconds),
      QueryClass.Execute      -> QueryTimeout(5.seconds),
      QueryClass.SelectIntersection         -> QueryTimeout(100.millis),
      QueryClass.SelectMetadata             -> QueryTimeout(100.millis),
      QueryClass.SelectMetadataIntersection -> QueryTimeout(100.millis)
    )
  }

  val materializingQueryEvaluator = edgesQueryEvaluator

  // schedulers

  class TestScheduler(val name: String) extends Scheduler {
    jobQueueName = name + "_jobs"

    val schedulerType = new KestrelScheduler {
      path = "/tmp"
      keepJournal = false
      maxMemorySize = 36.megabytes
    }

    threads = 2
    errorLimit = 25
    errorRetryDelay = 900.seconds
    errorStrobeInterval = 30.seconds
    perFlushItemLimit = 1000
    jitterRate = 0.0f
    badJobQueue = new JsonJobLogger { name = "bad_jobs" }
  }

  val jobQueues = Map(
    Priority.High.id   -> new TestScheduler("edges"),
    Priority.Medium.id -> new TestScheduler("copy"),
    Priority.Low.id    -> new TestScheduler("edges_slow")
  )


  // Admin/Logging

  val adminConfig = new AdminServiceConfig {
    httpPort = Some(9990)
  }

  loggers = List(new LoggerConfig {
    level = Some(Level.FATAL)
  })
}
