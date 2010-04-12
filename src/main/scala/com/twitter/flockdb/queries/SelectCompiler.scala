package com.twitter.flockdb.queries

import scala.collection.mutable
import com.twitter.gizzard.Future
import com.twitter.service.flock.State
import com.twitter.service.flock.thrift.FlockException
import flockdb.operations.{SelectOperation, SelectOperationType}


class InvalidQueryException(reason: String) extends FlockException(reason)

class SelectCompiler(forwardingManager: ForwardingManager) {
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
        stack.push(new IntersectionQuery(stack.pop, stack.pop))
      case SelectOperationType.Union =>
        if (stack.size < 2) throw new InvalidQueryException("Need two sub-queries to do a union")
        stack.push(new UnionQuery(stack.pop, stack.pop))
      case SelectOperationType.Difference =>
        if (stack.size < 2) throw new InvalidQueryException("Need two sub-queries to do a difference")
        val rightSide = stack.pop
        val leftSide = stack.pop
        stack.push(new DifferenceQuery(leftSide, rightSide))
      case n =>
        throw new InvalidQueryException("Unknown operation " + n)
    }
    if (stack.size != 1) throw new InvalidQueryException("Left " + stack.size + " items on the stack instead of 1")
    stack.pop
  }
}
