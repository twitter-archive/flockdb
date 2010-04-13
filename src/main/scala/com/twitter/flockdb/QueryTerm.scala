package com.twitter.flockdb


case class QueryTerm(sourceId: Long, graphId: Int, isForward: Boolean,
                     destinationIds: Option[Seq[Long]], var states: Seq[State])
