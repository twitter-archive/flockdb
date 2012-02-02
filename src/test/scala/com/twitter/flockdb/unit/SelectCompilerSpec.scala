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
import com.twitter.util.{Future, Time}
import com.twitter.util.TimeConversions._
import org.specs.mock.{ClassMocker, JMocker}
import com.twitter.flockdb
import com.twitter.flockdb.{Page => FlockPage}
import queries.{SelectCompiler, InvalidQueryException}
import operations.{SelectOperation, SelectOperationType}
import shards.Shard
import thrift.{Page, Results}


object SelectCompilerSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  "SelectCompiler" should {
    var forwardingManager: ForwardingManager = null
    var shard: Shard = null
    var shard2: Shard = null
    var selectCompiler: SelectCompiler = null
    val sourceId = 900
    val graphId = 5
    val states = new mutable.ArrayBuffer[State] {
      override def equals(that: Any) = that match {
        case that: Seq[_] => this.toList == that.toList
        case that => false
      }
    }
    states += State.Normal

    doBefore {
      forwardingManager = mock[ForwardingManager]
      shard = mock[Shard]
      shard2 = mock[Shard]
      selectCompiler = new SelectCompiler(forwardingManager, new flockdb.config.IntersectionQuery { averageIntersectionProportion = 1.0 })
    }

    "execute a simple wildcard query" in {
      "when the state is given" >> {
        expect {
          one(forwardingManager).find(sourceId, graphId, Direction.Forward) willReturn shard
          one(shard).count(sourceId, states) willReturn Future(23)
        }
        val program = new SelectOperation(SelectOperationType.SimpleQuery, Some(new QueryTerm(sourceId, graphId, true, None, List(State.Normal)))) :: Nil
        val query = selectCompiler(program)
        query.getClass.getName mustMatch "SimpleQuery"
        query.sizeEstimate()() mustEqual 23
      }
    }

    "should throw" in {
      "on an empty query" in {
         val program = Nil
         selectCompiler(program) must throwA[InvalidQueryException]
      }

      "on a malformed binary operation query" in {
        val program = new SelectOperation(SelectOperationType.SimpleQuery, Some(new QueryTerm(sourceId, graphId, true, None, List(State.Normal)))) ::
          new SelectOperation(SelectOperationType.Intersection, None) :: Nil
        selectCompiler(program) must throwA[InvalidQueryException]
      }

      "on a malformed dual-literal query" in {
        val program = new SelectOperation(SelectOperationType.SimpleQuery, Some(new QueryTerm(sourceId, graphId, true, None, List(State.Normal)))) ::
          new SelectOperation(SelectOperationType.SimpleQuery, Some(new QueryTerm(sourceId, graphId, true, None, List(State.Normal)))) :: Nil
        selectCompiler(program) must throwA[InvalidQueryException]
      }
    }


    "execute a simple list query" in {
      expect {
        one(forwardingManager).find(sourceId, graphId, Direction.Forward) willReturn shard
      }
      val program = new SelectOperation(SelectOperationType.SimpleQuery, Some(new QueryTerm(sourceId, graphId, true, Some(List[Long](12, 13)), List(State.Normal)))) :: Nil
      val query = selectCompiler(program)
      query.getClass.getName mustMatch "WhereInQuery"
      query.sizeEstimate()() mustEqual 2
    }

    "execute a compound query" in {
      expect {
        one(forwardingManager).find(sourceId, graphId, Direction.Forward) willReturn shard
        one(forwardingManager).find(sourceId, graphId, Direction.Backward) willReturn shard
        one(shard).count(sourceId, states) willReturn Future(23)
      }
      val program = new SelectOperation(SelectOperationType.SimpleQuery, Some(new QueryTerm(sourceId, graphId, true, None, List(State.Normal)))) ::
        new SelectOperation(SelectOperationType.SimpleQuery, Some(new QueryTerm(sourceId, graphId, false, Some(List[Long](12, 13)), List(State.Normal)))) ::
        new SelectOperation(SelectOperationType.Intersection, None) :: Nil
      val query = selectCompiler(program)
      query.getClass.getName mustMatch "IntersectionQuery"
      query.sizeEstimate()() mustEqual 2
    }

    "execute a nested compound query" in {
      expect {
        one(forwardingManager).find(sourceId, graphId, Direction.Forward) willReturn shard
        one(forwardingManager).find(sourceId, graphId, Direction.Backward) willReturn shard
        one(forwardingManager).find(sourceId + 1, graphId, Direction.Forward) willReturn shard2
        one(shard).count(sourceId, states) willReturn Future(23)
        one(shard2).count(sourceId + 1, states) willReturn Future(25)
      }
      val program = new SelectOperation(SelectOperationType.SimpleQuery, Some(new QueryTerm(sourceId, graphId, true, None, List(State.Normal)))) ::
        new SelectOperation(SelectOperationType.SimpleQuery, Some(new QueryTerm(sourceId, graphId, false, Some(List[Long](12, 13)), List(State.Normal)))) ::
        new SelectOperation(SelectOperationType.Intersection, None) ::
        new SelectOperation(SelectOperationType.SimpleQuery, Some(new QueryTerm(sourceId + 1, graphId, true, None, List(State.Normal)))) ::
        new SelectOperation(SelectOperationType.Union, None) :: Nil
      val query = selectCompiler(program)
      query.getClass.getName mustMatch "UnionQuery"
      query.sizeEstimate()() mustEqual 25
    }

    "execute a difference query in the right order" in {
      expect {
        one(forwardingManager).find(sourceId, graphId, Direction.Forward) willReturn shard
        one(forwardingManager).find(sourceId + 1, graphId, Direction.Forward) willReturn shard2
        one(shard).count(sourceId, states) willReturn Future(10)
        allowing(shard2).count(sourceId + 1, states) willReturn Future(2)
      }
      val program = new SelectOperation(SelectOperationType.SimpleQuery, Some(new QueryTerm(sourceId, graphId, true, None, List(State.Normal)))) ::
        new SelectOperation(SelectOperationType.SimpleQuery, Some(new QueryTerm(sourceId + 1, graphId, true, None, List(State.Normal)))) ::
        new SelectOperation(SelectOperationType.Difference, None) :: Nil
      val query = selectCompiler(program)
      query.getClass.getName mustMatch "DifferenceQuery"
      query.sizeEstimate()() mustEqual 10
    }


    "time a simple list query" in {
      expect {
        one(forwardingManager).find(sourceId, graphId, Direction.Forward) willReturn shard
        one(shard).intersect(sourceId, List(State.Normal), List[Long](12, 13)) willReturn Future(List[Long](12,13))
      }
      val program = new SelectOperation(SelectOperationType.SimpleQuery, Some(new QueryTerm(sourceId, graphId, true, Some(List[Long](12, 13)), List(State.Normal)))) :: Nil
      val queryTree = selectCompiler(program)
      queryTree.toString mustEqual "<WhereInQuery sourceId="+sourceId+" states=(Normal) destIds=(12,13)>"
      val rv = queryTree.select(FlockPage(0,Cursor(0)))()
      queryTree.toString mustMatch "time"
    }
  }
}
