package com.twitter.flockdb

import com.twitter.service.flock.State


case class QueryTerm(sourceId: Long, graphId: Int, isForward: Boolean,
                     destinationIds: Option[Seq[Long]], var states: Seq[State])
