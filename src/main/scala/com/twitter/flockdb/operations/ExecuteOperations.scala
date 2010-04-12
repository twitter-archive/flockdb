package com.twitter.flockdb.operations


case class ExecuteOperations(operations: Seq[ExecuteOperation], executeAt: Option[Int],
                             priority: Priority.Value)
