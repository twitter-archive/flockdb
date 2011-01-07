package com.twitter.flockdb

import com.twitter.util.Time


case class UnsafeAddQuery(sourceId: Long, graphId: Int, destinationId: Long, createdAt: Time)
