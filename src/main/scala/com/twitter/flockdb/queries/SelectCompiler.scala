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

package com.twitter.flockdb.queries

import scala.collection.mutable
import com.twitter.gizzard.Future
import operations.{SelectOperation, SelectOperationType}
import thrift.FlockException


class InvalidQueryException(reason: String) extends FlockException(reason)

class SelectCompiler(forwardingManager: ForwardingManager, intersectionConfig: config.IntersectionQuery) {
  def apply(program: Seq[SelectOperation]): Query = {
    val stack = new mutable.Stack[Query]

    for (op <- program) op.operationType match {
      case SelectOperationType.SimpleQuery =>
        val term = op.term.get
        val shard = forwardingManager.find(term.sourceId, term.graphId, Direction(term.isForward))
        val states = if (term.states.isEmpty) List(State.Normal) else term.states
        val query = if (term.destinationIds.isDefined) {
          new WhereInQuery(shard, term.sourceId, states, term.destinationIds.get)
        } else {
          new SimpleQuery(shard, term.sourceId, states)
        }
        stack.push(query)
      case SelectOperationType.Intersection =>
        if (stack.size < 2) throw new InvalidQueryException("Need two sub-queries to do an intersection")
        stack.push(intersectionConfig.intersect(stack.pop, stack.pop))
      case SelectOperationType.Union =>
        if (stack.size < 2) throw new InvalidQueryException("Need two sub-queries to do a union")
        stack.push(new UnionQuery(stack.pop, stack.pop))
      case SelectOperationType.Difference =>
        if (stack.size < 2) throw new InvalidQueryException("Need two sub-queries to do a difference")
        val rightSide = stack.pop
        val leftSide = stack.pop
        stack.push(intersectionConfig.difference(leftSide, rightSide))
      case n =>
        throw new InvalidQueryException("Unknown operation " + n)
    }
    if (stack.size != 1) throw new InvalidQueryException("Left " + stack.size + " items on the stack instead of 1")
    stack.pop
  }
}
