package com.twitter.flockdb

trait Repairable[T] extends JobSchedulable {
  def similar(other: T): Int
}