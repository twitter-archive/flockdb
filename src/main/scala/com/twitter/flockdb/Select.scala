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

import com.twitter.conversions.time._
import com.twitter.util.Time
import com.twitter.flockdb.operations.SelectOperationType._
import com.twitter.flockdb.operations._


object Select {
  def apply(sourceId: Unit, graphId: Int, destinationId: Long) = {
    new SimpleSelect(new SelectOperation(SimpleQuery, Some(new QueryTerm(destinationId, graphId, false, None, List(State.Normal)))))
  }

  def apply(sourceId: Long, graphId: Int, destinationId: Unit) = {
    new SimpleSelect(new SelectOperation(SimpleQuery, Some(new QueryTerm(sourceId, graphId, true, None, List(State.Normal)))))
  }

  def apply(sourceId: Long, graphId: Int, destinationId: Long) = {
    new SimpleSelect(new SelectOperation(SimpleQuery, Some(new QueryTerm(sourceId, graphId, true, Some(List[Long](destinationId)), List(State.Normal)))))
  }

  def apply(sourceId: Long, graphId: Int, destinationIds: Seq[Long]) = {
    new SimpleSelect(new SelectOperation(SimpleQuery, Some(new QueryTerm(sourceId, graphId, true, Some(destinationIds), List(State.Normal)))))
  }

  def apply(sourceIds: Seq[Long], graphId: Int, destinationId: Long) = {
    new SimpleSelect(new SelectOperation(SimpleQuery, Some(new QueryTerm(destinationId, graphId, false, Some(sourceIds), List(State.Normal)))))
  }
}

trait Select {
  def toList: List[SelectOperation]
  def intersect(that: Select): Select = new CompoundSelect(Intersection, this, that)
  def difference(that: Select): Select = new CompoundSelect(Difference, this, that)
}

trait Execute {
  def toOperations: List[ExecuteOperation]
  def at(time: Time): Execute
  def +(execute: Execute): Execute
}

// FIXME this is infinity-select not null :)
object NullSelect extends Select {
  override def intersect(that: Select) = that
  def toList = { throw new Exception("Not Applicable") }
}

case class SimpleSelect(operation: SelectOperation) extends Select {
  def toList = List(operation)

  def addAt(at: Time) = execute(ExecuteOperationType.Add, at)
  def add = addAt(Time.now)
  def archiveAt(at: Time) = execute(ExecuteOperationType.Archive, at)
  def archive = archiveAt(Time.now)
  def removeAt(at: Time) = execute(ExecuteOperationType.Remove, at)
  def remove = removeAt(Time.now)
  def negateAt(at: Time) = execute(ExecuteOperationType.Negate, at)
  def negate = negateAt(Time.now)
  private def execute(executeOperationType: ExecuteOperationType.Value, at: Time) =
    new SimpleExecute(new ExecuteOperation(executeOperationType, operation.term.get,
                                           Some(Time.now.inMillis)), at)

  def negative = {
    val negativeOperation = operation.clone
    negativeOperation.term.get.states = List(State.Negative)
    new SimpleSelect(negativeOperation)
  }

  def states(states: State*) = {
    val statefulOperation = operation.clone
    statefulOperation.term.get.states = states
    new SimpleSelect(statefulOperation)
  }
}

case class CompoundSelect(operation: SelectOperationType.Value, operand1: Select, operand2: Select) extends Select {
  def toList = operand1.toList ++ operand2.toList ++ List(new SelectOperation(operation, None))
}

case class SimpleExecute(operation: ExecuteOperation, at: Time) extends Execute {
  def toOperations = List(operation)
  def at(time: Time) = new SimpleExecute(operation, time)
  def +(execute: Execute) = new CompoundExecute(this, execute, at, Priority.High)
}

case class CompoundExecute(operand1: Execute, operand2: Execute, at: Time, priority: Priority.Value) extends Execute {
  def toOperations = operand1.toOperations ++ operand2.toOperations

  def +(execute: Execute) = new CompoundExecute(this, execute, at, priority)
  def withPriority(priority: Priority.Value) = new CompoundExecute(operand2, operand2, at, priority)
  def at(time: Time) = new CompoundExecute(operand1, operand2, time, priority)
}
