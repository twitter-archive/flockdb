package com.twitter.flockdb

trait Repairable[T] extends JobSchedulable with Ordered[T] {
  def similar(other: T): Int
}