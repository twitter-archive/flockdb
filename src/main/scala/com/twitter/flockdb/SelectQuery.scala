package com.twitter.flockdb

import com.twitter.results.Page
import operations.SelectOperation


case class SelectQuery(operations: Seq[SelectOperation], page: Page)
