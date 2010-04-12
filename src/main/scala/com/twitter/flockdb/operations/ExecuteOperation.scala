package com.twitter.flockdb.operations


case class ExecuteOperation(operationType: ExecuteOperationType.Value, term: QueryTerm,
                            position: Option[Long])
