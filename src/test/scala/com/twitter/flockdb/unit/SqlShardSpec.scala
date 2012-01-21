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
package unit

import java.sql.SQLException
import scala.collection.mutable
import com.twitter.gizzard.shards.{Busy, ShardId, ShardInfo}
import com.twitter.querulous.evaluator.{QueryEvaluator, QueryEvaluatorFactory, StandardQueryEvaluatorFactory, Transaction}
import com.twitter.querulous.query.SqlQueryFactory
import com.twitter.util.Time
import com.twitter.conversions.time._
import org.specs.mock.JMocker
import shards.{Shard, SqlShard, SqlShardFactory}

class SqlShardSpec extends IntegrationSpecification with JMocker {
  "Edge SqlShard" should {
    val alice = 1L
    val bob = 2L
    val carl = 3L
    val darcy = 4L
    val earl = 5L
    val frank = 6L

    val now = Time.now

    val queryEvaluatorFactory = config.edgesQueryEvaluator()
    val queryEvaluator = queryEvaluatorFactory(config.databaseConnection)
    val shardFactory = new SqlShardFactory(queryEvaluatorFactory, queryEvaluatorFactory, queryEvaluatorFactory, config.databaseConnection)
    val shardInfo = ShardInfo(ShardId("localhost", "table_001"), "com.twitter.flockdb.SqlShard",
      "INT UNSIGNED", "INT UNSIGNED", Busy.Normal)
    var shard: Shard = null

    doBefore {
      try {
        reset(config, config.databaseConnection.database)
        shardFactory.materialize(shardInfo)
        shard = shardFactory.instantiate(shardInfo, 1)
      } catch { case e => e.printStackTrace() }
    }

    "create" in {
      val createShardFactory = new SqlShardFactory(queryEvaluatorFactory, queryEvaluatorFactory, queryEvaluatorFactory, config.databaseConnection)
      val createShardInfo = ShardInfo(ShardId("localhost", "create_test"), "com.twitter.flockdb.SqlShard",
        "INT UNSIGNED", "INT UNSIGNED", Busy.Normal)
      val createShard = new SqlShard(createShardInfo, queryEvaluator, queryEvaluator, 0)

      "when the database doesn't exist" >> {
        createShardFactory.materialize(createShardInfo)
        queryEvaluator.select("SELECT * FROM create_test_edges") { row => row }().isEmpty mustBe true
        queryEvaluator.select("SELECT * FROM create_test_metadata") { row => row }().isEmpty mustBe true
      }

       "when the database does exist but the table doesn't exist" >> {
         createShardFactory.materialize(createShardInfo)
         queryEvaluator.select("SELECT * FROM create_test_edges") { row => row }().isEmpty mustBe true
       }

      "when both the database and table already exist" >> {
        createShardFactory.materialize(createShardInfo)
        createShardFactory.materialize(createShardInfo)
        queryEvaluator.select("SELECT * FROM create_test_edges") { row => row }().isEmpty mustBe true
      }
    }

    "count" in {
      "when the state is normal" >> {
        "when the count is materialized" >> {
          shard.add(alice, bob, 1, now)()
          shard.add(alice, carl, 2, now)()
          shard.add(carl, alice, 1, now)()
          shard.count(alice, List(State.Normal))() mustEqual 2
          shard.count(carl, List(State.Normal))() mustEqual 1
        }

        "when the user does not exist yet" >> {
          shard.count(bob, List(State.Normal))() mustEqual 0
        }

        "when the count is not materialized and user has deleted rows" >> {
          shard.remove(alice, bob, 1, now)()
          shard.count(alice, List(State.Normal))() mustEqual 0
        }

        "when the state is given" >> {
          "when no edges have been added beforehand and a non-normal state is given" >> {
            shard.count(alice, List(State.Archived))() mustEqual 0
            val metadata = shard.getMetadata(alice)().get
            metadata.state mustEqual State.Normal
          }

          "when edges have been added beforehand" >> {
            shard.add(alice, bob, 1, now)()
            shard.add(alice, carl, 2, now)()
            shard.remove(alice, darcy, 3, now)()
            shard.archive(alice, earl, 4, now)()
            shard.count(alice, List(State.Normal))() mustEqual 2
            shard.count(alice, List(State.Removed))() mustEqual 0
            shard.count(alice, List(State.Archived))() mustEqual 0
          }
        }
      }

      "when the state is not normal" >> {
        "when the same edge is added and removed multiple times" >> {
          shard.negate(alice, now)()
          shard.negate(alice, bob, 1, now)()
          shard.remove(alice, bob, 2, now + 1.second)()
          shard.negate(alice, bob, 3, now + 2.seconds)()
          shard.remove(alice, bob, 4, now + 3.seconds)()
          shard.count(alice, List(State.Negative))() mustEqual 0
        }

        "when an insert operation with the same state occurs" >> {
          shard.remove(alice, now)()
          shard.remove(alice, bob, 1, now)()
          shard.count(alice, List(State.Removed))() mustEqual 1
        }

        "when an update operation with the same state occurs" >> {
          shard.remove(alice, now)()
          shard.archive(alice, bob, 1, now)()
          shard.remove(alice, bob, 1, now + 1.second)()
          shard.count(alice, List(State.Removed))() mustEqual 1
        }
      }

      "multiple states" >> {
        shard.archive(alice, bob, 1, now)()
        shard.remove(alice, carl, 2, now)()
        shard.add(alice, darcy, 3, now)()
        // temporarily, all counts should be 0 that are not the state the metadata is in
        shard.count(alice, List(State.Archived, State.Removed, State.Normal))() mustEqual 1
      }

      "when the state transitions" >> {
        shard.add(alice, bob, 1, now)()
        shard.remove(alice, carl, 2, now)()
        shard.remove(alice, darcy, 3, now)()

        shard.remove(alice, now + 1.second)()
        shard.count(alice, List(State.Normal))() mustEqual 0
        shard.count(alice, List(State.Removed))() mustEqual 2
      }
    }

    "get" in {
      shard.add(alice, bob, 1, now)()
      shard.add(alice, carl, 2, now)()
      shard.add(carl, darcy, 1, now)()

      shard.get(alice, bob)() must beSome[Edge].which { _.position == 1 }
      shard.get(alice, carl)() must beSome[Edge].which { _.position == 2 }
      shard.get(alice, darcy)() mustBe None
      shard.get(alice, earl)() mustBe None
      shard.get(carl, darcy)() must beSome[Edge].which { _.position == 1 }
    }

    "intersect" in {
      "with state Normal" >> {
        shard.add(alice, bob, 1, now)()
        shard.add(alice, carl, 2, now)()
        shard.add(carl, darcy, 1, now)()
        shard.remove(alice, darcy, 3, now)()

        shard.intersect(alice, List(State.Normal), Nil)().toList mustEqual List()
        shard.intersect(alice, List(State.Normal), List(alice, bob, carl, darcy))().toList mustEqual List(carl, bob)
        shard.intersect(alice, List(State.Removed), List(bob, carl, darcy))().toList mustEqual List(darcy)
        shard.intersect(alice, List(State.Normal), List(alice, bob, darcy))().toList mustEqual List(bob)
        shard.intersect(alice, List(State.Normal), List(alice, darcy))().toList mustEqual Nil
        shard.intersect(bob, List(State.Normal), List(alice, bob, carl, darcy))().toList mustEqual Nil
      }
    }

    "selectAll" in {
      "all at once" >> {
        shard.add(alice, bob, 1, now)()
        shard.archive(alice, carl, 1, now)()

        val rows = new mutable.ArrayBuffer[Edge]
        rows ++= List(shard.get(alice, bob)().get, shard.get(alice, carl)().get)
        shard.selectAll((Cursor.Start, Cursor.Start), 10)() mustEqual (rows, (Cursor.End, Cursor.End))
      }

      "in two chunks" >> {
        shard.add(alice, bob, 1, now)()
        shard.archive(alice, carl, 2, now)()
        shard.remove(alice, darcy, 3, now)()
        shard.add(alice, earl, 4, now)()
        shard.add(carl, darcy, 1, now)()
        shard.add(earl, darcy, 1, now)()

        val rows = new mutable.ArrayBuffer[Edge]
        rows ++= List(shard.get(alice, bob)().get, shard.get(alice, carl)().get, shard.get(alice, darcy)().get)
        val (result, cursor) = shard.selectAll((Cursor.Start, Cursor.Start), 3)()
        result mustEqual rows
        cursor mustEqual (Cursor(alice), Cursor(darcy))

        rows.clear()
        rows ++= List(shard.get(alice, earl)().get, shard.get(carl, darcy)().get, shard.get(earl, darcy)().get)
        shard.selectAll(cursor, 3)() mustEqual (rows, (Cursor.End, Cursor.End))
      }
    }

    "select" in {
      "order by position" >> {
        "pagination" >> {
          shard.add(alice, bob, 3, now)()
          shard.add(alice, carl, 5, now)()
          shard.add(carl, darcy, 1, now)()

          shard.selectByPosition(alice, List(State.Normal), 1, Cursor.Start)().toTuple mustEqual (List(carl), Cursor(5), Cursor.End)
          shard.selectByPosition(alice, List(State.Normal), 5, Cursor.Start)().toTuple mustEqual (List(carl, bob), Cursor.End, Cursor.End)
          shard.selectByPosition(alice, List(State.Normal), 1, Cursor(5))().toTuple    mustEqual (List(bob), Cursor.End, Cursor(-3))
          shard.selectByPosition(alice, List(State.Normal), 1, Cursor(4))().toTuple    mustEqual (List(bob), Cursor.End, Cursor(-3))
          shard.selectByPosition(alice, List(State.Normal), 3, Cursor(4))().toTuple    mustEqual (List(bob), Cursor.End, Cursor(-3))
          shard.selectByPosition(bob, List(State.Normal), 5, Cursor.Start)().toTuple   mustEqual (List(), Cursor.End, Cursor.End)

          shard.selectByPosition(alice, List(State.Normal), 1, Cursor(-5))().toTuple   mustEqual (List(), Cursor.End, Cursor.End)
          shard.selectByPosition(alice, List(State.Normal), 1, Cursor(-3))().toTuple   mustEqual (List(carl), Cursor(5), Cursor.End)
          shard.selectByPosition(alice, List(State.Normal), 1, Cursor(-4))().toTuple   mustEqual (List(carl), Cursor(5), Cursor.End)
          shard.selectByPosition(alice, List(State.Normal), 1, Cursor(-2))().toTuple   mustEqual (List(bob), Cursor.End, Cursor(-3))
          shard.selectByPosition(alice, List(State.Normal), 3, Cursor(-2))().toTuple   mustEqual (List(carl, bob), Cursor.End, Cursor.End)
        }

        "when the state is given" >> {
          shard.add(alice, bob, 1, now)()
          shard.remove(alice, carl, 2, now)()
          shard.archive(alice, darcy, 3, now)()
          shard.selectByPosition(alice, List(State.Normal), 1, Cursor.Start)().toTuple mustEqual (List(bob), Cursor.End, Cursor.End)
          shard.selectByPosition(alice, List(State.Removed), 1, Cursor.Start)().toTuple mustEqual (List(carl), Cursor.End, Cursor.End)
          shard.selectByPosition(alice, List(State.Archived), 1, Cursor.Start)().toTuple mustEqual (List(darcy), Cursor.End, Cursor.End)
        }

        "with multiple allowed states" >> {
          shard.add(alice, bob, 1, now)()
          shard.remove(alice, carl, 2, now - 1.second)()
          shard.archive(alice, darcy, 3, now - 2.seconds)()

          shard.selectByPosition(alice, List(State.Normal, State.Removed), 10, Cursor.Start)().page.map { _.id }.toList mustEqual List(carl, bob)
          shard.selectByPosition(alice, List(State.Removed, State.Archived), 10, Cursor.Start)().page.map { _.id }.toList mustEqual List(darcy, carl)
          shard.selectByPosition(alice, List(State.Archived), 10, Cursor.Start)().page.map { _.id }.toList mustEqual List(darcy)
        }
      }

      "order by destination_id" >> {
        "pagination" >> {
          shard.add(alice, bob, 1, now)()
          shard.add(alice, carl, 2, now)()
          shard.add(carl, darcy, 1, now)()

          shard.selectByDestinationId(alice, List(State.Normal), 1, Cursor.Start)().toTuple mustEqual (List(carl), Cursor(carl), Cursor.End)
          shard.selectByDestinationId(alice, List(State.Normal), 5, Cursor.Start)().toTuple mustEqual (List(carl, bob), Cursor.End, Cursor.End)
          shard.selectByDestinationId(alice, List(State.Normal), 1, Cursor(carl))().toTuple mustEqual (List(bob), Cursor.End, Cursor(-bob))
          shard.selectByDestinationId(alice, List(State.Normal), 1, Cursor(-bob))().toTuple mustEqual (List(carl), Cursor(carl), Cursor.End)
        }

        "when the state is given" >> {
          shard.add(alice, bob, 1, now)()
          shard.remove(alice, carl, 2, now)()
          shard.archive(alice, darcy, 3, now)()

          shard.selectByDestinationId(alice, List(State.Normal), 1, Cursor.Start)().toTuple mustEqual (List(bob), Cursor.End, Cursor.End)
          shard.selectByDestinationId(alice, List(State.Removed), 1, Cursor.Start)().toTuple mustEqual (List(carl), Cursor.End, Cursor.End)
          shard.selectByDestinationId(alice, List(State.Archived), 1, Cursor.Start)().toTuple mustEqual (List(darcy), Cursor.End, Cursor.End)
        }
      }

      "includingArchived" >> {
        shard.add(alice, bob, 1, now)()
        shard.add(alice, carl, 2, now)()
        shard.add(carl, darcy, 1, now)()
        shard.archive(alice, earl, 1, now)()

        shard.selectIncludingArchived(alice, 5, Cursor.Start)().toTuple mustEqual (List(earl, carl, bob), Cursor.End, Cursor.End)
      }

      "get edge objects" >> {
        shard.add(alice, bob, 3, now)()
        shard.add(alice, carl, 5, now)()

        val aliceBob  = new Edge(alice, bob, 3, now, 0, State.Normal)
        val aliceCarl = new Edge(alice, carl, 5, now, 0, State.Normal)

        shard.selectEdges(alice, List(State.Normal), 1, Cursor.Start)().toTuple mustEqual (List(aliceCarl), Cursor(5), Cursor.End)
        shard.selectEdges(alice, List(State.Normal), 5, Cursor.Start)().toTuple mustEqual (List(aliceCarl, aliceBob), Cursor.End, Cursor.End)
        shard.selectEdges(alice, List(State.Normal), 1, Cursor(5))().toTuple    mustEqual (List(aliceBob), Cursor.End, Cursor(-3))
        shard.selectEdges(alice, List(State.Normal), 1, Cursor(4))().toTuple    mustEqual (List(aliceBob), Cursor.End, Cursor(-3))
        shard.selectEdges(alice, List(State.Normal), 3, Cursor(4))().toTuple    mustEqual (List(aliceBob), Cursor.End, Cursor(-3))
        shard.selectEdges(bob, List(State.Normal), 5, Cursor.Start)().toTuple   mustEqual (Nil, Cursor.End, Cursor.End)

        shard.selectEdges(alice, List(State.Normal), 1, Cursor(-5))().toTuple   mustEqual (Nil, Cursor.End, Cursor.End)
        shard.selectEdges(alice, List(State.Normal), 1, Cursor(-3))().toTuple   mustEqual (List(aliceCarl), Cursor(5), Cursor.End)
        shard.selectEdges(alice, List(State.Normal), 1, Cursor(-4))().toTuple   mustEqual (List(aliceCarl), Cursor(5), Cursor.End)
        shard.selectEdges(alice, List(State.Normal), 1, Cursor(-2))().toTuple   mustEqual (List(aliceBob), Cursor.End, Cursor(-3))
        shard.selectEdges(alice, List(State.Normal), 3, Cursor(-2))().toTuple   mustEqual (List(aliceCarl, aliceBob), Cursor.End, Cursor.End)
      }
    }

    "get" in {
      shard.add(alice, bob, 1, now)()
      shard.archive(carl, darcy, 2, now)()
      shard.remove(darcy, alice, 3, now)()

      shard.get(bob, alice)() mustEqual None
      shard.get(alice, bob)() mustEqual Some(Edge(alice, bob, 1, now, 0, State.Normal))
      shard.get(carl, darcy)() mustEqual Some(Edge(carl, darcy, 2, now, 0, State.Archived))
      shard.get(darcy, alice)() mustEqual Some(Edge(darcy, alice, 3, now, 0, State.Removed))
    }

    "add" in {
      "creates an edge" >> {
        "when the row does not already exist" >> {
          shard.get(bob, alice)() mustEqual None
          shard.add(bob, alice, 1, now)()
          shard.get(bob, alice)() mustEqual Some(Edge(bob, alice, 1, now, 0, State.Normal))
        }

        "when the row already exists" >> {
          "when the already-existing row is older than the row to be inserted" >> {

            // Flock-fix redefines a re-insert as a no-op
            "when the already existing row is not deleted" >> {
              shard.add(alice, bob, 1, now)()

              shard.add(alice, bob, 2, now + 10.seconds)()
              shard.get(alice, bob)() mustEqual Some(Edge(alice, bob, 1, now + 10.seconds, 0, State.Normal))
            }

            "when the already existing row is not archived" >> {
              shard.archive(alice, bob, 1, now)()

              shard.add(alice, bob, 2, now + 10.seconds)()
              shard.get(alice, bob)() mustEqual Some(Edge(alice, bob, 1, now + 10.seconds, 0, State.Normal))
            }
          }

          "when the already-existing row is newer than the row to be inserted" >> {
            shard.add(alice, bob, 1, now)()
            shard.add(alice, bob, 1, now - 1.second)()
            shard.get(alice, bob)().get mustEqual Edge(alice, bob, 1, now, 0, State.Normal)
          }

          "when the already-existing row is the same age as the row to be inserted" >> {
            "when the already-existing row is deleted"  >> {
              shard.remove(alice, bob, 1, now)()
              shard.add(alice, bob, 1, now)()

              shard.get(alice, bob)() mustEqual Some(Edge(alice, bob, 1, now, 0, State.Removed))
            }

            "when the already-existing row is archived" >> {
              shard.archive(alice, bob, 1, now)()
              shard.add(alice, bob, 1, now)()

              shard.get(alice, bob)() mustEqual Some(Edge(alice, bob, 1, now, 0, State.Archived))
            }

            "when the already-existing row is negative" >> {
              shard.negate(alice, bob, 1, now)()
              shard.add(alice, bob, 1, now)()

              shard.get(alice, bob)() mustEqual Some(Edge(alice, bob, 1, now, 0, State.Negative))
            }
          }
        }
      }

      "when the state is normal" >> {
        "increments a count" >> {
          "when the same row is inserted twice" >> {
            shard.add(alice, earl, 1, now + 5.seconds)()
            shard.add(alice, earl, 1, now + 6.seconds)()
            shard.count(alice, List(State.Normal))() mustBe 1
          }

          "when a row is inserted once" >> {
            shard.add(alice, earl, 1, now)()
            shard.count(alice, List(State.Normal))() mustBe 1
          }
        }

        "when the already-existing row is newer than the row to be inserted" >> {
          "when the row was not already deleted" >> {
             shard.add(alice, bob, 1, now + 1.second)()
             shard.add(alice, bob, 1, now)()
             shard.count(alice, List(State.Normal))() mustBe 1
          }

          "when the row was already deleted" >> {
            shard.remove(alice, bob, 1, now + 1.seconds)()
            shard.count(alice, List(State.Normal))() mustBe 0
            shard.add(alice, bob, 1, now)()
            shard.count(alice, List(State.Normal))() mustBe 0
          }
        }
      }
    }

    "remove" in {
      "when the row does not exist" >> {
        shard.remove(bob, alice, 1, now)()
        shard.get(bob, alice)() mustEqual Some(Edge(bob, alice, 1, now, 0, State.Removed))
      }

      "when the row exists" >> {
        "when the already-existing row is older than the row to be deleted" >> {
          "when the already existing row is not deleted" >> {
            shard.add(alice, bob, 1, now)()
            shard.remove(alice, bob, 2, now + 10.seconds)()
            shard.get(alice, bob)() mustEqual Some(Edge(alice, bob, 1, now + 10.seconds, 0, State.Removed))
          }
        }

        "when the already-existing row is newer than the row to be deleted" >> {
          shard.add(carl, darcy, 1, now)()
          shard.remove(carl, darcy, 1, now - 1.second)()
          shard.get(carl, darcy)() mustEqual Some(Edge(carl, darcy, 1, now, 0, State.Normal))
        }

      }

      "decrements a count" >> {
        "when the row doesn't exist" >> {
          shard.remove(alice, bob, 1, now)()
          shard.count(alice, List(State.Normal))() mustBe 0
        }

        "when the row already exists" >> {
          shard.add(alice, bob, 1, now)()
          shard.add(alice, carl, 2, now)()
          shard.remove(alice, bob, 1, now + 1.second)()
          shard.count(alice, List(State.Normal))() mustBe 1
        }

        "when the already-existing row is newer than the row to be deleted" >> {
          shard.add(alice, bob, 1, now)()
          shard.remove(alice, bob, 1, now - 1.second)()
          shard.count(alice, List(State.Normal))() mustBe 1
        }
      }
    }

    "remove & add" in {
      "incremements the count when deleting then re-inserting a row" >> {
        shard.remove(carl, darcy, 1, now)()
        shard.get(carl, darcy)() mustEqual Some(Edge(carl, darcy, 1, now, 0, State.Removed))
        shard.add(carl, darcy, 1, now + 1.second)()
        shard.get(carl, darcy)() mustEqual Some(Edge(carl, darcy, 1, now + 1.second, 0, State.Normal))
      }

      "when the remove is applied before the add, but its updatedAt is greater than the add" >> {
        shard.remove(carl, earl, 1, now)()
        shard.add(carl, earl, 1, now - 1.second)()
        shard.get(carl, earl)() mustEqual Some(Edge(carl, earl, 1, now, 0, State.Removed))
      }

      "when the deleting an already deleted row" >> {
        shard.remove(alice, bob, 1, now)()
        shard.remove(alice, bob, 1, now + 2.second)()
        shard.add(alice, bob, 1, now + 1.seconds)()
        shard.get(alice, bob)() mustEqual Some(Edge(alice, bob, 1, now + 2.second, 0, State.Removed))
      }
    }

    "archive" in {
      "when the row does not exist" >> {
        shard.archive(bob, alice, 1, now)()
        shard.get(bob, alice)() mustEqual Some(Edge(bob, alice, 1, now, 0, State.Archived))
      }

      "when the row exists" >> {
        "when the already-existing row is older than the row to be archived" >> {
          "when the already existing row is not archived or deleted" >> {
            shard.add(alice, bob, 1, now)()
            shard.archive(alice, bob, 1, now + 1.second)()
            shard.get(alice, bob)() mustEqual Some(Edge(alice, bob, 1, now + 1.second, 0, State.Archived))
          }
        }

        "when the already-existing row is newer than the row to be archived" >> {
          shard.add(alice, bob, 1, now)()
          shard.archive(alice, bob, 1, now - 1.second)()
          shard.get(alice, bob)() mustEqual Some(Edge(alice, bob, 1, now, 0, State.Normal))
        }

        "when the already-existing row is the same age as the row to be archived" >> {
          "when the already-existing row is removed" >> {
            shard.remove(alice, bob, 1, now)()
            shard.archive(alice, bob, 1, now)()
            shard.get(alice, bob)() mustEqual Some(Edge(alice, bob, 1, now, 0, State.Removed))
          }

          "when the already-existing row is removed" >> {
            shard.remove(alice, bob, 1, now)()
            shard.archive(alice, bob, 1, now)()
            shard.get(alice, bob)() mustEqual Some(Edge(alice, bob, 1, now, 0, State.Removed))
          }
        }
      }

      "decrements a count" >> {
        "when the user already has a materialized count" >> {
          shard.add(alice, bob, 1, now)()
          shard.add(alice, carl, 2, now)()
          shard.archive(alice, bob, 1, now + 1.seconds)()
          shard.count(alice, List(State.Normal))() mustEqual 1
        }

        "when the already-existing row is newer than the row to be archived" >> {
          shard.add(alice, bob, 1, now)()
          shard.archive(alice, bob, 1, now - 1.second)()
          shard.count(alice, List(State.Normal))() mustEqual 1
        }
      }
    }

    "archive & add" in {
      "incremements the count when archiving then re-inserting a row" >> {
        shard.add(alice, bob, 1, now)()
        shard.archive(alice, bob, 1, now + 1.second)()
        shard.get(alice, bob)() mustEqual Some(Edge(alice, bob, 1, now + 1.second, 0, State.Archived))
        shard.add(alice, bob, 1, now + 2.seconds)()
        shard.get(alice, bob)() mustEqual Some(Edge(alice, bob, 1, now + 2.seconds, 0, State.Normal))
      }

      "when the archive is applied before the add, but its updatedAt is greater than the add" >> {
        shard.archive(alice, bob, 1, now)()
        shard.add(alice, bob, 1, now - 1.second)()
        shard.get(alice, bob)() mustEqual Some(Edge(alice, bob, 1, now, 0, State.Archived))
      }

      "when the archive an already archived row" >> {
        shard.add(alice, bob, 1, now)()
        shard.archive(alice, bob, 1, now)()
        shard.get(alice, bob)() mustEqual Some(Edge(alice, bob, 1, now, 0, State.Archived))
      }
    }

    "archive & remove" in {
      "counts stay correct" >> {
        shard.add(alice, carl, 1, now)()

        shard.remove(alice, bob, 1, now)()
        shard.remove(alice, bob, 1, now + 1.second)()
        shard.archive(alice, bob, 1, now + 2.seconds)()
        shard.count(alice, List(State.Normal))() mustEqual 1
      }
    }

    "metadata" in {
      "order of metadata changes is respected" >> {
        shard.add(alice, now)()
        shard.archive(alice, now - 1.second)()
        val metadata = shard.getMetadata(alice)().get
        metadata.state mustBe State.Normal
      }

      "two simultaneous metadata changes" >> {
        "normal vs. removed" >> {
          shard.add(alice, now)()
          shard.remove(alice, now)()
          shard.add(alice, now)()
          val metadata = shard.getMetadata(alice)().get
          metadata.state mustBe State.Removed
        }

        "normal vs. archived" >> {
          shard.add(alice, now)()
          shard.archive(alice, now)()
          shard.add(alice, now)()
          val metadata = shard.getMetadata(alice)().get
          metadata.state mustBe State.Archived
        }

        "normal vs. negative" >> {
          shard.add(alice, now)()
          shard.negate(alice, now)()
          shard.add(alice, now)()
          val metadata = shard.getMetadata(alice)().get
          metadata.state mustBe State.Negative
        }

        "negative vs. archived" >> {
          shard.negate(alice, now)()
          shard.archive(alice, now)()
          shard.negate(alice, now)()
          val metadata = shard.getMetadata(alice)().get
          metadata.state mustBe State.Archived
        }

        "archived vs. removed" >> {
          shard.archive(alice, now)()
          shard.remove(alice, now)()
          shard.archive(alice, now)()
          val metadata = shard.getMetadata(alice)().get
          metadata.state mustBe State.Removed
        }
      }

      "row changes don't update metadata" >> {
        shard.archive(alice, now)()
        shard.add(alice, bob, 1, now + 1.second)()
        val metadata = shard.getMetadata(alice)().get
        metadata.state mustBe State.Archived
        metadata.updatedAtSeconds mustEqual now.inSeconds
      }

      "a row change simultaneous with a metadata update does not win" >> {
        shard.add(alice, bob, 1, now)()
        shard.archive(alice, now)()
        val metadata = shard.getMetadata(alice)().get
        metadata.state mustBe State.Archived
      }

      "write always creates" in {
        val metadata = Metadata(alice, State.Normal, 0, now - 10.seconds)
        val olderMetadata = Metadata(alice, State.Normal, 0, now - 20.seconds)

        shard.writeMetadata(metadata)()
        shard.getMetadata(alice)() mustEqual Some(metadata)
        shard.writeMetadata(olderMetadata)()
        shard.getMetadata(alice)() mustEqual Some(metadata)
      }

      "write can also update" in {
        val metadata = Metadata(bob, State.Normal, 0, now - 10.seconds)
        val olderMetadata = Metadata(bob, State.Normal, 0, now - 20.seconds)

        shard.writeMetadata(olderMetadata)()
        shard.getMetadata(bob)() mustEqual Some(olderMetadata)
        shard.writeMetadata(metadata)()
        shard.getMetadata(bob)() mustEqual Some(metadata)
      }
    }

    "writeCopies" in {
      "simple" in {
        val edge = Edge(alice, bob, 1, now, 1, State.Normal)
        shard.writeCopies(List(edge))()
        shard.get(alice, bob)() mustEqual Some(edge)
      }

      "multiple" in {
        val edges = Edge(alice, bob, 1, now, 1, State.Normal) ::
          Edge(alice, darcy, 2, now, 1, State.Normal) ::
          Edge(bob, carl, 3, now, 1, State.Normal) ::
          Edge(frank, bob, 4, now, 1, State.Normal) ::
          Edge(frank, carl, 5, now, 1, State.Normal) ::
          Edge(frank, darcy, 6, now, 1, State.Normal) ::
          Nil

        "no conflicts" in {
          shard.writeCopies(edges)()
          shard.get(alice, bob)() mustEqual Some(edges(0))
          shard.get(alice, darcy)() mustEqual Some(edges(1))
          shard.get(bob, carl)() mustEqual Some(edges(2))
          shard.get(frank, bob)() mustEqual Some(edges(3))
          shard.get(frank, carl)() mustEqual Some(edges(4))
          shard.get(frank, darcy)() mustEqual Some(edges(5))
        }

        "conflicts" in {
          shard.add(frank, carl, 5, now)()
          shard.writeCopies(edges)()
          shard.get(alice, bob)() mustEqual Some(edges(0))
          shard.get(alice, darcy)() mustEqual Some(edges(1))
          shard.get(bob, carl)() mustEqual Some(edges(2))
          shard.get(frank, bob)() mustEqual Some(edges(3))
          shard.get(frank, carl)() must beSome[Edge]
          shard.get(frank, darcy)() mustEqual Some(edges(5))
        }

        "retries edges that failed a bulk-insert" in {
          val stubShard = new SqlShard(shardInfo, queryEvaluator, queryEvaluator, 0) {
            override def writeBurst(transaction: Transaction, edges: Seq[Edge]) = {
              val completed = new mutable.ArrayBuffer[Edge]
              val failed = new mutable.ArrayBuffer[Edge]
              edges.foreach { edge =>
                if (edge.destinationId == darcy) {
                  failed += edge
                } else {
                  completed += edge
                }
              }
              BurstResult(completed, failed)
            }
          }

          stubShard.writeCopies(edges)()
          shard.get(alice, darcy)() mustEqual Some(edges(1))
          shard.get(frank, darcy)() mustEqual Some(edges(5))
        }
      }
    }
  }
}
