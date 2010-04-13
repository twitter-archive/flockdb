package com.twitter.flockdb.unit

import scala.collection.jcl.Conversions._
import scala.collection.mutable
import com.twitter.xrayspecs.Time
import com.twitter.xrayspecs.TimeConversions._
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}
import queries.SelectCompiler
import operations.{SelectOperation, SelectOperationType}
import shards.Shard
import thrift.{Page, Results}


object SelectCompilerSpec extends Specification with JMocker with ClassMocker {
  "SelectCompiler" should {
    var forwardingManager: ForwardingManager = null
    var shard: Shard = null
    var shard2: Shard = null
    var selectCompiler: SelectCompiler = null
    val sourceId = 900
    val graphId = 5
    val states = new mutable.ArrayBuffer[State] {
      override def equals(that: Any) = that match {
        case that: Seq[State] => this.toList == that.toList
        case that => false
      }
    }
    states += State.Normal

    doBefore {
      forwardingManager = mock[ForwardingManager]
      shard = mock[Shard]
      shard2 = mock[Shard]
      selectCompiler = new SelectCompiler(forwardingManager)
    }

    "execute a simple wildcard query" in {
      "when the state is given" >> {
        expect {
          one(forwardingManager).find(sourceId, graphId, Direction.Forward) willReturn shard
          one(shard).count(sourceId, states) willReturn 23
        }
        val program = new SelectOperation(SelectOperationType.SimpleQuery, Some(new QueryTerm(sourceId, graphId, true, None, List(State.Normal)))) :: Nil
        val query = selectCompiler(program)
        query.getClass.getName mustMatch "SimpleQuery"
        query.sizeEstimate mustEqual 23
      }
    }

    "execute a simple list query" in {
      expect {
        one(forwardingManager).find(sourceId, graphId, Direction.Forward) willReturn shard
      }
      val program = new SelectOperation(SelectOperationType.SimpleQuery, Some(new QueryTerm(sourceId, graphId, true, Some(List[Long](12, 13)), List(State.Normal)))) :: Nil
      val query = selectCompiler(program)
      query.getClass.getName mustMatch "WhereInQuery"
      query.sizeEstimate mustEqual 2
    }

    "execute a compound query" in {
      expect {
        one(forwardingManager).find(sourceId, graphId, Direction.Forward) willReturn shard
        one(forwardingManager).find(sourceId, graphId, Direction.Backward) willReturn shard
        one(shard).count(sourceId, states) willReturn 23
      }
      val program = new SelectOperation(SelectOperationType.SimpleQuery, Some(new QueryTerm(sourceId, graphId, true, None, List(State.Normal)))) ::
        new SelectOperation(SelectOperationType.SimpleQuery, Some(new QueryTerm(sourceId, graphId, false, Some(List[Long](12, 13)), List(State.Normal)))) ::
        new SelectOperation(SelectOperationType.Intersection, None) :: Nil
      val query = selectCompiler(program)
      query.getClass.getName mustMatch "IntersectionQuery"
      (query.asInstanceOf[queries.IntersectionQuery]).config("edges.average_intersection_proportion") = "1.0"
      query.sizeEstimate mustEqual 2
    }

    "execute a nested compound query" in {
      expect {
        one(forwardingManager).find(sourceId, graphId, Direction.Forward) willReturn shard
        one(forwardingManager).find(sourceId, graphId, Direction.Backward) willReturn shard
        one(forwardingManager).find(sourceId + 1, graphId, Direction.Forward) willReturn shard2
        one(shard).count(sourceId, states) willReturn 23
        one(shard2).count(sourceId + 1, states) willReturn 25
      }
      val program = new SelectOperation(SelectOperationType.SimpleQuery, Some(new QueryTerm(sourceId, graphId, true, None, List(State.Normal)))) ::
        new SelectOperation(SelectOperationType.SimpleQuery, Some(new QueryTerm(sourceId, graphId, false, Some(List[Long](12, 13)), List(State.Normal)))) ::
        new SelectOperation(SelectOperationType.Intersection, None) ::
        new SelectOperation(SelectOperationType.SimpleQuery, Some(new QueryTerm(sourceId + 1, graphId, true, None, List(State.Normal)))) ::
        new SelectOperation(SelectOperationType.Union, None) :: Nil
      val query = selectCompiler(program)
      query.getClass.getName mustMatch "UnionQuery"
      query.sizeEstimate mustEqual 25
    }

    "execute a difference query in the right order" in {
      expect {
        one(forwardingManager).find(sourceId, graphId, Direction.Forward) willReturn shard
        one(forwardingManager).find(sourceId + 1, graphId, Direction.Forward) willReturn shard2
        one(shard).count(sourceId, states) willReturn 10
        allowing(shard2).count(sourceId + 1, states) willReturn 2
      }
      val program = new SelectOperation(SelectOperationType.SimpleQuery, Some(new QueryTerm(sourceId, graphId, true, None, List(State.Normal)))) ::
        new SelectOperation(SelectOperationType.SimpleQuery, Some(new QueryTerm(sourceId + 1, graphId, true, None, List(State.Normal)))) ::
        new SelectOperation(SelectOperationType.Difference, None) :: Nil
      val query = selectCompiler(program)
      query.getClass.getName mustMatch "DifferenceQuery"
      query.sizeEstimate mustEqual 10
    }
  }
}
