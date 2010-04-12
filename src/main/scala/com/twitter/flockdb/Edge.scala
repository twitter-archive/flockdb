package com.twitter.flockdb

import com.twitter.xrayspecs.Time
import com.twitter.service.flock.State


case class Edge(sourceId: Long, destinationId: Long, position: Long, updatedAt: Time, count: Int,
                state: State)
