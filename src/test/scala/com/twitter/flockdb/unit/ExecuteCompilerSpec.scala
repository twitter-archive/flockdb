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

import scala.collection.mutable
import com.twitter.gizzard.nameserver.InvalidShard
import com.twitter.gizzard.scheduler.{JsonJob, JsonNestedJob, PrioritizingJobScheduler}
import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.util.Time
import com.twitter.util.TimeConversions._
import org.specs.mock.{ClassMocker, JMocker}
import jobs.single
import jobs.multi
import queries.ExecuteCompiler
import operations.{ExecuteOperations, ExecuteOperation, ExecuteOperationType}


object ExecuteCompilerSpec extends ConfiguredSpecification with JMocker with ClassMocker {

  val now = Time.now

  def termToProgram(operationType: ExecuteOperationType.Value, term: QueryTerm, time: Option[Time], position: Option[Long]): ExecuteOperations = {
    val operation = new ExecuteOperation(operationType, term, position)
    val operations = List(operation)
    new ExecuteOperations(operations, time.map { _.inSeconds }, Priority.Low)
  }

  def termToProgram(operationType: ExecuteOperationType.Value, term: QueryTerm): ExecuteOperations = termToProgram(operationType, term, Some(now))
  def termToProgram(operationType: ExecuteOperationType.Value, term: QueryTerm, time: Option[Time]): ExecuteOperations = termToProgram(operationType, term, time, Some(now.inMillis))

  "ExecuteCompiler" should {
    val FOLLOWS = 1

    val alice = 1L
    val bob = 2L
    val carl = 3L
    var scheduler: PrioritizingJobScheduler = null
    var executeCompiler: ExecuteCompiler = null
    var forwardingManager: ForwardingManager = null
    val nestedJob = capturingParam[JsonNestedJob]

    doBefore {
      scheduler = mock[PrioritizingJobScheduler]
      forwardingManager = mock[ForwardingManager]
      executeCompiler = new ExecuteCompiler(scheduler, forwardingManager, config.aggregateJobsPageSize, IdentityUuidGenerator)
    }

    "without execute_at present" in {
      Time.withCurrentTimeFrozen { time =>
        val program = termToProgram(ExecuteOperationType.Add, new QueryTerm(alice, FOLLOWS, true, Some(List[Long](bob)), List(State.Normal)), None)
        expect {
          one(forwardingManager).find(0, FOLLOWS, Direction.Forward)
          one(scheduler).put(will(beEqual(Priority.Low.id)), nestedJob.capture)
        }
        executeCompiler(program)
        jsonMatching(List(single.Add(alice, FOLLOWS, bob, now.inMillis, Time.now, null, null)), nestedJob.captured.jobs)
      }
    }

    "without position present" in {
      Time.withCurrentTimeFrozen { time =>
        val program = termToProgram(ExecuteOperationType.Add, new QueryTerm(alice, FOLLOWS, true, Some(List[Long](bob)), List(State.Normal)), None, None)
        expect {
          one(forwardingManager).find(0, FOLLOWS, Direction.Forward)
          one(scheduler).put(will(beEqual(Priority.Low.id)), nestedJob.capture)
        }
        executeCompiler(program)
        jsonMatching(List(single.Add(alice, FOLLOWS, bob, Time.now.inMillis, Time.now, null, null)), nestedJob.captured.jobs)
      }
    }

    "with an invalid graph" in {
      val program = termToProgram(ExecuteOperationType.Add, new QueryTerm(alice, FOLLOWS, true, Some(List[Long](bob)), List(State.Normal)), None)
      expect {
        one(forwardingManager).find(0, FOLLOWS, Direction.Forward) willThrow(new InvalidShard("message"))
      }
      executeCompiler(program) must throwA[InvalidShard]
    }

    "compile add operations" in {
      "single" >> {
        "forward" >> {
          val program = termToProgram(ExecuteOperationType.Add, new QueryTerm(alice, FOLLOWS, true, Some(List[Long](bob)), List(State.Normal)))
          expect {
            one(forwardingManager).find(0, FOLLOWS, Direction.Forward)
            one(scheduler).put(will(beEqual(Priority.Low.id)), nestedJob.capture)
          }
          executeCompiler(program)
          jsonMatching(List(single.Add(alice, FOLLOWS, bob, now.inMillis, now, null, null)), nestedJob.captured.jobs)
        }

        "backward" >> {
          val program = termToProgram(ExecuteOperationType.Add, new QueryTerm(alice, FOLLOWS, false, Some(List[Long](bob)), List(State.Normal)))
          expect {
            one(forwardingManager).find(0, FOLLOWS, Direction.Forward)
            one(scheduler).put(will(beEqual(Priority.Low.id)), nestedJob.capture)
          }
          executeCompiler(program)
          jsonMatching(List(single.Add(bob, FOLLOWS, alice, now.inMillis, now, null, null)), nestedJob.captured.jobs)
        }
      }

      "aggregate" >> {
        "forward" >> {
          val program = termToProgram(ExecuteOperationType.Add, new QueryTerm(alice, FOLLOWS, true, None, List(State.Normal)))
          expect {
            one(forwardingManager).find(0, FOLLOWS, Direction.Forward)
            one(scheduler).put(will(beEqual(Priority.Low.id)), nestedJob.capture)
          }
          executeCompiler(program)
          jsonMatching(List(multi.Unarchive(alice, FOLLOWS, Direction.Forward, now, Priority.Low, config.aggregateJobsPageSize, null, null, IdentityUuidGenerator)), nestedJob.captured.jobs)
        }

        "backward" >> {
          val program = termToProgram(ExecuteOperationType.Add, new QueryTerm(alice, FOLLOWS, false, None, List(State.Normal)))
          expect {
            one(forwardingManager).find(0, FOLLOWS, Direction.Forward)
            one(scheduler).put(will(beEqual(Priority.Low.id)), nestedJob.capture)
          }
          executeCompiler(program)
          jsonMatching(List(multi.Unarchive(alice, FOLLOWS, Direction.Backward, now, Priority.Low, config.aggregateJobsPageSize, null, null, IdentityUuidGenerator)), nestedJob.captured.jobs)
        }
      }

      "multi" >> {
        "forward" >> {
          val program = termToProgram(ExecuteOperationType.Add, new QueryTerm(alice, FOLLOWS, true, Some(List[Long](bob, carl)), List(State.Normal)))
          expect {
            one(forwardingManager).find(0, FOLLOWS, Direction.Forward)
            one(scheduler).put(will(beEqual(Priority.Low.id)), nestedJob.capture)
          }
          executeCompiler(program)
          jsonMatching(List(
            single.Add(alice, FOLLOWS, bob, now.inMillis, now, null, null),
            single.Add(alice, FOLLOWS, carl, now.inMillis, now, null, null)), nestedJob.captured.jobs)
        }

        "backward" >> {
          val program = termToProgram(ExecuteOperationType.Add, new QueryTerm(alice, FOLLOWS, false, Some(List[Long](bob, carl)), List(State.Normal)))
          expect {
            one(forwardingManager).find(0, FOLLOWS, Direction.Forward)
            one(scheduler).put(will(beEqual(Priority.Low.id)), nestedJob.capture)
          }
          executeCompiler(program)
          jsonMatching(List(
            single.Add(bob, FOLLOWS, alice, now.inMillis, now, null, null),
            single.Add(carl, FOLLOWS, alice, now.inMillis, now, null, null)), nestedJob.captured.jobs)
        }
      }
    }

    "compile remove operations" in {
      "single" >> {
        "forward" >> {
          val program = termToProgram(ExecuteOperationType.Remove, new QueryTerm(alice, FOLLOWS, true, Some(List[Long](bob)), List(State.Normal)))
          expect {
            one(forwardingManager).find(0, FOLLOWS, Direction.Forward)
            one(scheduler).put(will(beEqual(Priority.Low.id)), nestedJob.capture)
          }
          executeCompiler(program)
          jsonMatching(List(new single.Remove(alice, FOLLOWS, bob, now.inMillis, now, null, null)), nestedJob.captured.jobs)
        }

        "backward" >> {
          val program = termToProgram(ExecuteOperationType.Remove, new QueryTerm(alice, FOLLOWS, false, Some(List[Long](bob)), List(State.Normal)))
          expect {
            one(forwardingManager).find(0, FOLLOWS, Direction.Forward)
            one(scheduler).put(will(beEqual(Priority.Low.id)), nestedJob.capture)
          }
          executeCompiler(program)
          jsonMatching(List(new single.Remove(bob, FOLLOWS, alice, now.inMillis, now, null, null)), nestedJob.captured.jobs)
        }
      }

      "aggregate" >> {
        "forward" >> {
          val program = termToProgram(ExecuteOperationType.Remove, new QueryTerm(alice, FOLLOWS, true, None, List(State.Normal)))
          expect {
            one(forwardingManager).find(0, FOLLOWS, Direction.Forward)
            one(scheduler).put(will(beEqual(Priority.Low.id)), nestedJob.capture)
          }
          executeCompiler(program)
          jsonMatching(List(multi.RemoveAll(alice, FOLLOWS, Direction.Forward, now, Priority.Low, config.aggregateJobsPageSize, null, null, IdentityUuidGenerator)), nestedJob.captured.jobs)
        }

        "backward" >> {
          val program = termToProgram(ExecuteOperationType.Remove, new QueryTerm(alice, FOLLOWS, false, None, List(State.Normal)))
          expect {
            one(forwardingManager).find(0, FOLLOWS, Direction.Forward)
            one(scheduler).put(will(beEqual(Priority.Low.id)), nestedJob.capture)
          }
          executeCompiler(program)
          jsonMatching(List(multi.RemoveAll(alice, FOLLOWS, Direction.Backward, now, Priority.Low, config.aggregateJobsPageSize, null, null, IdentityUuidGenerator)), nestedJob.captured.jobs)
        }
      }

      "multi" >> {
        "forward" >> {
          val program = termToProgram(ExecuteOperationType.Remove, new QueryTerm(alice, FOLLOWS, true, Some(List[Long](bob, carl)), List(State.Normal)))
          expect {
            one(forwardingManager).find(0, FOLLOWS, Direction.Forward)
            one(scheduler).put(will(beEqual(Priority.Low.id)), nestedJob.capture)
          }
          executeCompiler(program)
          jsonMatching(List(
            new single.Remove(alice, FOLLOWS, bob, now.inMillis, now, null, null),
            new single.Remove(alice, FOLLOWS, carl, now.inMillis, now, null, null)), nestedJob.captured.jobs)
        }

        "backward" >> {
          val program = termToProgram(ExecuteOperationType.Remove, new QueryTerm(alice, FOLLOWS, false, Some(List[Long](bob, carl)), List(State.Normal)))
          expect {
            one(forwardingManager).find(0, FOLLOWS, Direction.Forward)
            one(scheduler).put(will(beEqual(Priority.Low.id)), nestedJob.capture)
          }
          executeCompiler(program)
          jsonMatching(List(
            new single.Remove(bob, FOLLOWS, alice, now.inMillis, now, null, null),
            new single.Remove(carl, FOLLOWS, alice, now.inMillis, now, null, null)), nestedJob.captured.jobs)
        }
      }
    }

    "compile archive operations" in {
      "single" >> {
        "forward" >> {
          val program = termToProgram(ExecuteOperationType.Archive, new QueryTerm(alice, FOLLOWS, true, Some(List[Long](bob)), List(State.Normal)))
          expect {
            one(forwardingManager).find(0, FOLLOWS, Direction.Forward)
            one(scheduler).put(will(beEqual(Priority.Low.id)), nestedJob.capture)
          }
          executeCompiler(program)
          jsonMatching(List(single.Archive(alice, FOLLOWS, bob, now.inMillis, now, null, null)), nestedJob.captured.jobs)
        }

        "backward" >> {
          val program = termToProgram(ExecuteOperationType.Archive, new QueryTerm(alice, FOLLOWS, false, Some(List[Long](bob)), List(State.Normal)))
          expect {
            one(forwardingManager).find(0, FOLLOWS, Direction.Forward)
            one(scheduler).put(will(beEqual(Priority.Low.id)), nestedJob.capture)
          }
          executeCompiler(program)
          jsonMatching(List(single.Archive(bob, FOLLOWS, alice, now.inMillis, now, null, null)), nestedJob.captured.jobs)
        }
      }

      "aggregate" >> {
        "forward" >> {
          val program = termToProgram(ExecuteOperationType.Archive, new QueryTerm(alice, FOLLOWS, true, None, List(State.Normal)))
          expect {
            one(forwardingManager).find(0, FOLLOWS, Direction.Forward)
            one(scheduler).put(will(beEqual(Priority.Low.id)), nestedJob.capture)
          }
          executeCompiler(program)
          jsonMatching(List(multi.Archive(alice, FOLLOWS, Direction.Forward, now, Priority.Low, config.aggregateJobsPageSize, null, null, IdentityUuidGenerator)), nestedJob.captured.jobs)
        }

        "backward" >> {
          val program = termToProgram(ExecuteOperationType.Archive, new QueryTerm(alice, FOLLOWS, false, None, List(State.Normal)))
          expect {
            one(forwardingManager).find(0, FOLLOWS, Direction.Forward)
            one(scheduler).put(will(beEqual(Priority.Low.id)), nestedJob.capture)
          }
          executeCompiler(program)
          jsonMatching(List(multi.Archive(alice, FOLLOWS, Direction.Backward, now, Priority.Low, config.aggregateJobsPageSize, null, null, IdentityUuidGenerator)), nestedJob.captured.jobs)
        }
      }

      "multi" >> {
        "forward" >> {
          val program = termToProgram(ExecuteOperationType.Archive, new QueryTerm(alice, FOLLOWS, true, Some(List[Long](bob, carl)), List(State.Normal)))
          expect {
            one(forwardingManager).find(0, FOLLOWS, Direction.Forward)
            one(scheduler).put(will(beEqual(Priority.Low.id)), nestedJob.capture)
          }
          executeCompiler(program)
          jsonMatching(List(
            single.Archive(alice, FOLLOWS, bob, now.inMillis, now, null, null),
            single.Archive(alice, FOLLOWS, carl, now.inMillis, now, null, null)), nestedJob.captured.jobs)
        }

        "backward" >> {
          val program = termToProgram(ExecuteOperationType.Archive, new QueryTerm(alice, FOLLOWS, false, Some(List[Long](bob, carl)), List(State.Normal)))
          expect {
            one(forwardingManager).find(0, FOLLOWS, Direction.Forward)
            one(scheduler).put(will(beEqual(Priority.Low.id)), nestedJob.capture)
          }
          executeCompiler(program)
          jsonMatching(List(
            single.Archive(bob, FOLLOWS, alice, now.inMillis, now, null, null),
            single.Archive(carl, FOLLOWS, alice, now.inMillis, now, null, null)), nestedJob.captured.jobs)
          }
      }
    }

    "negate" >> {
      "single" >> {
        "forward" >> {
          val program = termToProgram(ExecuteOperationType.Negate, new QueryTerm(alice, FOLLOWS, true, Some(List[Long](bob)), List(State.Normal)))
          expect {
            one(forwardingManager).find(0, FOLLOWS, Direction.Forward)
            one(scheduler).put(will(beEqual(Priority.Low.id)), nestedJob.capture)
          }
          executeCompiler(program)
          jsonMatching(List(single.Negate(alice, FOLLOWS, bob, now.inMillis, now, null, null)), nestedJob.captured.jobs)
        }

        "backward" >> {
          val program = termToProgram(ExecuteOperationType.Negate, new QueryTerm(alice, FOLLOWS, false, Some(List[Long](bob)), List(State.Normal)))
          expect {
            one(forwardingManager).find(0, FOLLOWS, Direction.Forward)
            one(scheduler).put(will(beEqual(Priority.Low.id)), nestedJob.capture)
          }
          executeCompiler(program)
          jsonMatching(List(single.Negate(bob, FOLLOWS, alice, now.inMillis, now, null, null)), nestedJob.captured.jobs)
        }
      }

      "aggregate" >> {
        "forward" >> {
          val program = termToProgram(ExecuteOperationType.Negate, new QueryTerm(alice, FOLLOWS, true, None, List(State.Normal)))
          expect {
            one(forwardingManager).find(0, FOLLOWS, Direction.Forward)
            one(scheduler).put(will(beEqual(Priority.Low.id)), nestedJob.capture)
          }
          executeCompiler(program)
          jsonMatching(List(multi.Negate(alice, FOLLOWS, Direction.Forward, now, Priority.Low, config.aggregateJobsPageSize, null, null, IdentityUuidGenerator)), nestedJob.captured.jobs)
        }

        "backward" >> {
          val program = termToProgram(ExecuteOperationType.Negate, new QueryTerm(alice, FOLLOWS, false, None, List(State.Normal)))
          expect {
            one(forwardingManager).find(0, FOLLOWS, Direction.Forward)
            one(scheduler).put(will(beEqual(Priority.Low.id)), nestedJob.capture)
          }
          executeCompiler(program)
          jsonMatching(List(multi.Negate(alice, FOLLOWS, Direction.Backward, now, Priority.Low, config.aggregateJobsPageSize, null, null, IdentityUuidGenerator)), nestedJob.captured.jobs)
        }
      }

      "multi" >> {
        "forward" >> {
          val program = termToProgram(ExecuteOperationType.Negate, new QueryTerm(alice, FOLLOWS, true, Some(List[Long](bob, carl)), List(State.Normal)))
          expect {
            one(forwardingManager).find(0, FOLLOWS, Direction.Forward)
            one(scheduler).put(will(beEqual(Priority.Low.id)), nestedJob.capture)
          }
          executeCompiler(program)
          jsonMatching(List(
            single.Negate(alice, FOLLOWS, bob, now.inMillis, now, null, null),
            single.Negate(alice, FOLLOWS, carl, now.inMillis, now, null, null)), nestedJob.captured.jobs)
        }

        "backward" >> {
          val program = termToProgram(ExecuteOperationType.Negate, new QueryTerm(alice, FOLLOWS, false, Some(List[Long](bob, carl)), List(State.Normal)))
          expect {
            one(forwardingManager).find(0, FOLLOWS, Direction.Forward)
            one(scheduler).put(will(beEqual(Priority.Low.id)), nestedJob.capture)
          }
          executeCompiler(program)
          jsonMatching(List(
            single.Negate(bob, FOLLOWS, alice, now.inMillis, now, null, null),
            single.Negate(carl, FOLLOWS, alice, now.inMillis, now, null, null)), nestedJob.captured.jobs)
        }
      }
    }
  }
}
