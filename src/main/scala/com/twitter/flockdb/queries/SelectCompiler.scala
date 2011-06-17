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
package queries

import scala.collection.mutable
import com.twitter.gizzard.{Stats, Future}
import operations.{SelectOperation, SelectOperationType}
import thrift.FlockException


class InvalidQueryException(reason: String) extends FlockException(reason)

class SelectCompiler(forwardingManager: ForwardingManager, intersectionConfig: config.IntersectionQuery) {

  private def validateProgram(acc: Int, op: SelectOperation) = op.operationType match {
    case SelectOperationType.SimpleQuery  => acc + 1
    case SelectOperationType.Intersection =>
      if (acc < 2) throw new InvalidQueryException("Need two sub-queries to do an intersection")
      acc - 1
    case SelectOperationType.Union        =>
      if (acc < 2) throw new InvalidQueryException("Need two sub-queries to do a union")
      acc - 1
    case SelectOperationType.Difference   =>
      if (acc < 2) throw new InvalidQueryException("Need two sub-queries to do a difference")
      acc - 1
    case n =>      throw new InvalidQueryException("Unknown operation " + n)
  }

  def apply(program: Seq[SelectOperation]): Query = {

    // program is a list representation of a compound query in reverse polish (postfix) notation
    // with one literal (SimpleQuery) and three binary operators (Intersection, Union, Difference)
    // left fold over list to ensure that a valid parsing exists
    val items = program.foldLeft(0)(validateProgram)
    if (items != 1) throw new InvalidQueryException("Left " + items + " items on the stack instaed of 1")

    var stack = new mutable.Stack[QueryTree]
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
        stack.push(intersectionConfig.intersect(stack.pop, stack.pop))
      case SelectOperationType.Union =>
        stack.push(new UnionQuery(stack.pop, stack.pop))
      case SelectOperationType.Difference =>
        val rightSide = stack.pop
        val leftSide = stack.pop
        stack.push(intersectionConfig.difference(leftSide, rightSide))
    }
    val rv = stack.pop

    // complexity == 0 indicates only a single literal (no binary operators) -- program is length 1
    val complexity = rv.getComplexity()
    val name = if (complexity > 0) {
      "select-complex-"+complexity
    } else {
      "select-" + (rv match {
        case query: WhereInQuery => if (query.sizeEstimate() == 1) "single" else "simple"
        case query: SimpleQuery  => if (program.head.term.get.states.size > 1) "-multistate" else ""
      })
    }

    Stats.transaction.record("Query Plan: "+rv.toString)
    Stats.transaction.name = name
    rv
  }
}
