import com.twitter.flockdb.config._
import com.twitter.gizzard.config._
import com.twitter.querulous.config._
import com.twitter.util.TimeConversions._
import com.twitter.flockdb.shards.QueryClass
import com.twitter.flockdb.Priority


trait Credentials extends Connection {
  val username = "root"
  val password = ""
}

trait DevelopmentQueryEvaluator extends QueryEvaluator {
  val autoDisable = Some(new AutoDisablingQueryEvaluator {
    val errorCount = 100
    val interval = 60.seconds
  })

  val database = new Database {
    val statsCollector = None
    val pool = Some(new ApachePoolingDatabase {
      override val sizeMin = 2
      override val sizeMax = 2
      override val maxWait = 100.millis
      override val minEvictableIdle = 60.seconds
      val testIdleMsec = 1.second
      override val testOnBorrow = false
    })

    val timeout = Some(new TimingOutDatabase {
      val poolSize = 10
      val queueSize = 10000
      val open = 100.millis
      val initialize = 1000.millis
    })
  }
}

new FlockDB {
  val server = new THsHaServer {
    val timeout = 100.millis
    val idleTimeout = 60.seconds
    val port = 7919
    val threadPool = new ThreadPool {
      val name = "flockdb_edges"
      val minThreads = 200
      override val maxThreads = 200
    }
  }

  val nameServer = new com.twitter.tbird.config.NameServer {
    val mappingFunction = Identity
    val replicas = Seq(new Mysql {
      val queryEvaluator  = new DevelopmentQueryEvaluator {
        val query = new Query {}
      }
      val connection = new Connection with Credentials {
        val hostnames = Seq("localhost")
        val database = "flock_edges_test"
      }
    })

    val jobRelay = Some(new JobRelay {
      val priority = Priority.High.id
      val framed   = false
      val timeout  = 1000.millis
    })
  }

  val jobInjector = new JobInjector with THsHaServer {
    val timeout = 100.milliseconds
    val idleTimeout = 60.seconds

    val threadPool = new ThreadPool {
      val name = "JobInjectorThreadPool"
      val minThreads = 5
    }
  }


  // futures

  val replicationFuture = new Future {
    val poolSize = 100
    val maxPoolSize = 100
    val keepAlive = 5.seconds
    val timeout = 6.seconds
  }

  val readFuture = new Future {
    val poolSize = 100
    val maxPoolSize = 100
    val keepAlive = 5.seconds
    val timeout = 500.millis
  }


  // Database Connectivity

  val databaseConnection = new Credentials {
    val hostnames = Seq("localhost")
    val database = "edges_test"
    override val urlOptions = Map(
      "rewriteBatchedStatements" -> "true"
    )
  }

  val edgesQueryEvaluator = new DevelopmentQueryEvaluator {
    val query = new Query {
      override val timeouts = Map(
        QueryClass.Select       -> QueryTimeout(100.millis),
        QueryClass.SelectModify -> QueryTimeout(5.seconds),
        QueryClass.SelectCopy   -> QueryTimeout(15.seconds),
        QueryClass.Execute      -> QueryTimeout(5.seconds)
      )
    }
  }


  // schedulers

  class DevelopmentScheduler(val name: String) extends Scheduler {
    override val jobQueueName = name + "_jobs"

    val schedulerType = new Kestrel {
      val queuePath = "/tmp"
      override val keepJournal = false
      override val maxMemorySize = 36000000L
    }

    val threads = 1
    val errorLimit = 25
    val errorRetryDelay = 900.seconds
    val errorStrobeInterval = 30.seconds
    val perFlushItemLimit = 1000
    val jitterRate = 0.0f
    val badJobQueue = Some(new JsonJobLogger { val name = "bad_jobs" })
  }

  val jobQueues = Map(
    Priority.High.id   -> new DevelopmentScheduler("edges"),
    Priority.Medium.id -> new DevelopmentScheduler("copy"),
    Priority.Low.id    -> new DevelopmentScheduler("edges_slow")
  )


  // Admin/Logging

  val adminConfig = new AdminConfig {
    val textPort = 9991
    val httpPort = 9990
  }

  override val logging = new LogConfigString("""
level = "fatal"
console = true
throttle_period_msec = 60000
throttle_rate = 10
""")

}
