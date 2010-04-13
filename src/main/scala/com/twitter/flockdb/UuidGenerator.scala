package com.twitter.flockdb

trait UuidGenerator extends (Long => Long) {
  def apply(updatedAt: Long): Long
  def unapply(uuid: Long): Option[Long]
}

object OrderedUuidGenerator extends UuidGenerator {
  private val randomGenerator = new util.Random
  // 64 bits - 20 leaves 44 bits of milliseconds, or over 500 years.
  private val unusedBits = 20
  private val randomMask = (1 << unusedBits) - 1

  def apply(updatedAt: Long) = {
    (updatedAt << unusedBits) | (randomGenerator.nextInt() & randomMask)
  }

  def unapply(uuid: Long) = {
    Some(uuid >> unusedBits)
  }
}

object IdentityUuidGenerator extends UuidGenerator {
  def apply(updatedAt: Long) = updatedAt

  def unapply(uuid: Long) = Some(uuid)
}
