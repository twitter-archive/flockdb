import com.twitter.flockdb.config._
import com.twitter.gizzard.config._
import com.twitter.gizzard.TransactionalStatsProvider
import com.twitter.querulous.config._
import com.twitter.querulous.database.DatabaseFactory
import com.twitter.querulous.query.QueryFactory
import com.twitter.querulous.StatsCollector
import com.twitter.conversions.time._
import com.twitter.conversions.storage._
import com.twitter.flockdb.shards.QueryClass
import com.twitter.flockdb.{MemoizedQueryEvaluators, Priority}
import com.twitter.ostrich.admin.config.AdminServiceConfig
import com.twitter.logging.{Level, Logger}
import com.twitter.logging.config.{FileHandlerConfig, LoggerConfig}


trait Credentials extends Connection {
  import scala.collection.JavaConversions._
  val env = System.getenv().toMap
  val username = env.get("DB_USERNAME").getOrElse("root")
  val password = env.get("DB_PASSWORD").getOrElse("")
  urlOptions = Map("connectTimeout" -> "0")
}

class TestQueryEvaluator(label: String) extends AsyncQueryEvaluator {
  query.debug = { s => Logger.get("query").debug(s) }
  override var workPoolSize = 2
  singletonFactory = true
  database.memoize = true
  database.pool = new ThrottledPoolingDatabase {
    size = workPoolSize
    openTimeout = 5.seconds
  }

  query.timeouts = Map(
    QueryClass.Select       -> QueryTimeout(5.seconds),
    QueryClass.SelectModify -> QueryTimeout(5.seconds),
    QueryClass.SelectCopy   -> QueryTimeout(15.seconds),
    QueryClass.Execute      -> QueryTimeout(5.seconds),
    QueryClass.SelectSingle -> QueryTimeout(5.seconds),
    QueryClass.SelectIntersection         -> QueryTimeout(5.seconds),
    QueryClass.SelectIntersectionSmall    -> QueryTimeout(5.seconds),
    QueryClass.SelectMetadata             -> QueryTimeout(5.seconds)
  )
}

class NameserverQueryEvaluator extends QueryEvaluator {
  singletonFactory = true
  database.memoize = true
  database.pool = new ThrottledPoolingDatabase {
    size = 1
    openTimeout = 5.seconds
  }
}

new FlockDB {
  mappingFunction = Identity
  jobRelay        = NoJobRelay

  nameServerReplicas = Seq(new Mysql {
    queryEvaluator  = new NameserverQueryEvaluator

    val connection = new Connection with Credentials {
      val hostnames = Seq("localhost")
      val database = "flock_edges_test"
    }
  })

  jobInjector.timeout               = 100.milliseconds
  jobInjector.idleTimeout           = 60.seconds
  jobInjector.threadPool.minThreads = 30

  // Database Connectivity

  val databaseConnection = new Credentials {
    val hostnames = Seq("localhost")
    val database = "edges_test"
  }

  val edgesQueryEvaluator = new TestQueryEvaluator("edges")
  val lowLatencyQueryEvaluator = edgesQueryEvaluator
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
    level = Some(Level.INFO)
    handlers = List(new FileHandlerConfig { filename = "test.log" })
  })

  queryStats.consumers = Seq(new AuditingTransactionalStatsConsumer {
    names = Set("execute")
    override def apply() = { new com.twitter.gizzard.AuditingTransactionalStatsConsumer(new com.twitter.gizzard.LoggingTransactionalStatsConsumer("audit_log") {
      def transactionToString(t: TransactionalStatsProvider) = { t.get("job").asInstanceOf[String] }
    }, names)}})
}
