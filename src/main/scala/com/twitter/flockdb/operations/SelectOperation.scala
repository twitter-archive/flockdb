package com.twitter.flockdb.operations


case class SelectOperation(operationType: SelectOperationType.Value, term: Option[QueryTerm]) {
  override def clone() = SelectOperation(operationType, term)
}
