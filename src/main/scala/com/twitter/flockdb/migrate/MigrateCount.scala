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

package com.twitter.flockdb.migrate

import java.sql.{ResultSet, SQLException}
import java.util.concurrent.{Executors, TimeUnit}
import scala.collection.mutable
import scala.util.Sorting
import com.twitter.gizzard.Future
import com.twitter.gizzard.nameserver
import com.twitter.gizzard.shards.ShardInfo
import com.twitter.ostrich.{Stats, W3CStats}
import com.twitter.querulous.StatsCollector
import com.twitter.querulous.evaluator.{QueryEvaluator, QueryEvaluatorFactory, Transaction}
import com.twitter.querulous.query.QueryClass
import net.lag.configgy.{Config, ConfigMap, Configgy, RuntimeEnvironment}
import net.lag.logging.Logger

class Runner(f: (QueryEvaluator, ShardInfo) => Unit, queryEvaluator: QueryEvaluator, shardInfo: ShardInfo) extends Runnable {
  def run() = f(queryEvaluator, shardInfo)
}

object ShardIterator {
  def apply(config: ConfigMap, nThreads: Int)(f: (QueryEvaluator, ShardInfo) => Unit) = {
    val executor = Executors.newFixedThreadPool(nThreads)

    val replicationFuture = new Future("ReplicationFuture", config.configMap("edges.replication.future"))

    val dbQueryEvaluatorFactory = QueryEvaluatorFactory.fromConfig(config.configMap("db"), None)

    val shardRepository = new nameserver.BasicShardRepository[shards.Shard](
      new shards.ReadWriteShardAdapter(_), None)
    shardRepository += ("com.twitter.flockdb.SqlShard" -> new shards.SqlShardFactory(dbQueryEvaluatorFactory, dbQueryEvaluatorFactory, config))

    val nameServer = nameserver.NameServer(config.configMap("edges.nameservers"), None,
                                           shardRepository, None)

    Sorting.stableSort(nameServer.listShards, { x: ShardInfo => x.tablePrefix }).foreach { shardInfo =>
      if (shardInfo.className == "com.twitter.flockdb.SqlShard") {
        val queryEvaluator = dbQueryEvaluatorFactory(List(shardInfo.hostname), config("edges.db_name"), config("db.username"), config("db.password"))
        executor.execute(new Runner(f, queryEvaluator, shardInfo))
      }
    }
    executor.shutdown
    executor.awaitTermination(7, TimeUnit.DAYS)
  }
}



object SchemaMigrateAddCounts {
  val baseQuery = "ALTER TABLE %s_metadata " +
    "ADD COLUMN count0 int(11) NOT NULL DEFAULT -1 AFTER count, " +
    "ADD COLUMN count1 int(11) NOT NULL DEFAULT -1 AFTER count0, " +
    "ADD COLUMN count2 int(11) NOT NULL DEFAULT -1 AFTER count1, " +
    "ADD COLUMN count3 int(11) NOT NULL DEFAULT -1 AFTER count2"
  def apply(config: ConfigMap) = {
    var errors = 0
    ShardIterator(config, 1) { (queryEvaluator, shardInfo) =>
      var tries = 0
      var ok = false
      while (tries < 3 && !ok) {
        try {
          printf("%s/%s... ", shardInfo.hostname, shardInfo.tablePrefix)
          Console.flush()
          val query = baseQuery.format(shardInfo.tablePrefix)
          SchemaMigrate.logQuery(query)
          queryEvaluator.execute(query)
          printf("ok\n")
          Console.flush()
          ok = true
          Thread.sleep(2000)
        } catch {
          case e: SQLException =>
            if (e.getMessage contains "Duplicate column name") {
              printf("already done.\n")
              ok = true
              Console.flush()
            } else {
              e.printStackTrace
              Thread.sleep(5000)
            }
        }
        tries += 1
      }
      if (!ok) {
        errors += 1
      }
    }
    if (errors > 0) {
      println("THERE WERE ERRORS: " + errors + " of them. You should run this script again.")
    } else {
      println("All done! :)")
    }
  }
}

object SchemaMigrateDropCounts {
  def apply(config: ConfigMap) = {
    ShardIterator(config, 1) {
      (queryEvaluator, shardInfo) => {
        try {
          queryEvaluator.execute("""
ALTER TABLE """ + shardInfo.tablePrefix + """_metadata DROP COLUMN count0, DROP COLUMN count1, DROP COLUMN count2, DROP COLUMN count3
""")
        } catch  {
          case e: SQLException => e.printStackTrace
        }
      }
    }
  }
}

object SchemaMigrateDumpSchema {
  def apply(config: ConfigMap) = {
    ShardIterator(config, 1) {
      (queryEvaluator, shardInfo) => {
        var numCountColumns = 0
        queryEvaluator.select("DESCRIBE " + shardInfo.tablePrefix + "_metadata") {
          row =>
            if (row.getString("Field").startsWith("count"))
              numCountColumns += 1
        }
        numCountColumns match {
          case 1 => println("Migration not done: " + shardInfo.tablePrefix)
          case 5 => {}
          case num: Int => println("Bad count " + numCountColumns.toString + ": " + shardInfo.tablePrefix)
        }
      }
    }
  }
}

object SchemaMigrateChangeCountsDefault {
  def apply(config: ConfigMap) = {
    ShardIterator(config, 10) {
      (queryEvaluator, shardInfo) => {
        queryEvaluator.execute("""
ALTER TABLE """ + shardInfo.tablePrefix + """_metadata
ALTER COLUMN count0 SET DEFAULT 0,
ALTER COLUMN count1 SET DEFAULT 0,
ALTER COLUMN count2 SET DEFAULT 0,
ALTER COLUMN count3 SET DEFAULT 0
""")
      }
    }
  }
}

object SchemaMigratePopulateCounts {
  def apply(config: ConfigMap) = {
    ShardIterator(config, 10) {
      (queryEvaluator, shardInfo) => {
        val schemaMigratePopulateCounts = new SchemaMigratePopulateCounts(queryEvaluator, shardInfo)
        schemaMigratePopulateCounts.migrateAllMetadata()
      }
    }
  }
}

class SchemaMigratePopulateCounts(private val queryEvaluator: QueryEvaluator, shardInfo: ShardInfo) {
  private def atomically(sourceId: Long)(f: Transaction => Unit) = {
    queryEvaluator.transaction { transaction =>
      transaction.selectOne("SELECT * FROM " + shardInfo.tablePrefix + "_metadata WHERE source_id = ? FOR UPDATE", sourceId) {
        row => f(transaction)
      }
    }
  }

  private def computeCounts(row: ResultSet) = {
    val results = mutable.Map[Int, Int]()
    val counts = (0 to 3).filter {
      count =>
        row.getInt("state") != count && row.getInt("count" + count) == -1
    }
    counts.foreach { count => results(count) = 0 }
    results(row.getInt("state")) = row.getInt("count")

    if (counts.length > 0) {
      queryEvaluator.select("SELECT state, count(*) AS c FROM " + shardInfo.tablePrefix + "_edges WHERE source_id = ? AND state IN (?) GROUP BY state", row.getLong("source_id"), counts) { row =>
        results(row.getInt("state")) = row.getInt("c") }
    }
    results
  }

  private def migrateMetadata(row: ResultSet) = {
    atomically(row.getLong("source_id")) { transaction => {
      val counts = computeCounts(row)

      if (counts.size > 0) {
        val countKeys = counts.keys.collect
        val countValues = countKeys.map { counts(_) }

        transaction.execute("UPDATE " + shardInfo.tablePrefix + "_metadata " +
                            "SET " +
                            countKeys.map { "count" + _ + " = GREATEST(?, 0)" }.mkString(", ") + " " +
                            "WHERE source_id = ?",
                            countValues ++ List(row.getLong("source_id")): _*)
      }
    }}
  }

  def migrateAllMetadata() = {
    val count = 10000
    var more = 1
    var cursor = Cursor(-1)
    do {
      more = 0
      queryEvaluator.select("SELECT * FROM " + shardInfo.tablePrefix + "_metadata WHERE (count0 = -1 OR count1 = -1 OR count2 = -1 OR count3 = -1) AND source_id > ? LIMIT ?", cursor.position, count) {
        row => {
          more = 1
          cursor = Cursor(row.getLong("source_id"))
          migrateMetadata(row)
        }
      }
    } while (more > 0)
  }
}


object SchemaMigrate {
  var verbose = false

  def logQuery(query: String) {
    if (verbose) {
      println("+ " + query)
    }
  }

  def parseCmdLine(args: Seq[String]) = {
    var cmd: Option[String] = None
    var i = 0
    while (i < args.size) {
      if (args(i) == "-v") {
        verbose = true
      } else if (cmd == None) {
        cmd = Some(args(i))
      }
      i += 1
    }
    cmd
  }

  def usage = {
    println("Usage: [-v] [addCounts|dropCounts|populateCounts|changeCountsDefault]")
  }

  def apply(args: Seq[String]) {
    QueryClass.register(shards.SelectModify)

    val cmd = parseCmdLine(args)

    if (cmd == None) {
      usage
    } else {
      val config = Configgy.config
      cmd.get match {
        case "addCounts" => SchemaMigrateAddCounts(config)
        case "dropCounts" => SchemaMigrateDropCounts(config)
        case "dumpSchema" => SchemaMigrateDumpSchema(config)
        case "populateCounts" => SchemaMigratePopulateCounts(config)
        case "changeCountsDefault" => SchemaMigrateChangeCountsDefault(config)
        case _ => usage
      }
    }
  }
}
