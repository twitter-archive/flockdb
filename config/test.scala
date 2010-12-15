import com.twitter.flockdb.config._
import com.twitter.gizzard.config._
import com.twitter.querulous.config._
import com.twitter.querulous.StatsCollector
import com.twitter.util.TimeConversions._
import com.twitter.flockdb.shards.QueryClass
import com.twitter.flockdb.{MemoizedQueryEvaluators, Priority}


trait Credentials extends Connection {
  val username = "root"
  val password = ""
}

class TestQueryEvaluator(label: String) extends QueryEvaluator {
  autoDisable = new AutoDisablingQueryEvaluator {
    val errorCount = 100
    val interval = 60.seconds
  }

//  query.debug = DebugLog
  database.memoize = true
  database.pool = new ApachePoolingDatabase {
    sizeMin = 2
    sizeMax = 2
    maxWait = 1.second
    minEvictableIdle = 60.seconds
    testIdle = 1.second
    testOnBorrow = false
  }

/*  database.timeout = new TimingOutDatabase {
    poolSize = 10
    queueSize = 10000
    open = 1.second
    initialize = 1.second
  } */

  override def apply(stats: StatsCollector) = {
    MemoizedQueryEvaluators.evaluators.getOrElseUpdate(label, { super.apply(stats) } )
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
      QueryClass.Execute      -> QueryTimeout(5.seconds)
    )
  }

  val materializingQueryEvaluator = edgesQueryEvaluator

  // schedulers

  class TestScheduler(val name: String) extends Scheduler {
    override val jobQueueName = name + "_jobs"

    val schedulerType = new KestrelScheduler {
      val queuePath = "/tmp"
      override val keepJournal = false
      override val maxMemorySize = 36000000L
    }

    threads = 1
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

  val adminConfig = new AdminConfig {
    val textPort = 9991
    val httpPort = 9990
  }

  logging = new LogConfigString("""
level = "fatal"
console = true
throttle_period_msec = 60000
throttle_rate = 10
""")

}
