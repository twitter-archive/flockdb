package com.twitter.flockdb

import com.twitter.xrayspecs.Time


case class Edge(sourceId: Long, destinationId: Long, position: Long, updatedAt: Time, count: Int,
                state: State)
