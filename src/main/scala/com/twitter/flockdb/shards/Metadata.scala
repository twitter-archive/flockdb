package com.twitter.flockdb.shards

import com.twitter.xrayspecs.Time

object Metadata {
  def apply(sourceId: Long, state: State, countOfCurrentState: Int, updatedAt: Time): Metadata = {
    state.id match {
      case 0 => Metadata(sourceId, state, countOfCurrentState, 0, 0, 0, updatedAt)
      case 1 => Metadata(sourceId, state, 0, countOfCurrentState, 0, 0, updatedAt)
      case 2 => Metadata(sourceId, state, 0, 0, countOfCurrentState, 0, updatedAt)
      case 3 => Metadata(sourceId, state, 0, 0, 0, countOfCurrentState, updatedAt)
    }
  }
}

case class Metadata(sourceId: Long, state: State, count0: Int, count1: Int, count2: Int, count3: Int, updatedAt: Time) 