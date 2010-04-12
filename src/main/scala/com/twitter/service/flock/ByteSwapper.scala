package com.twitter.service.flock

object ByteSwapper extends (Long => Long) {
  def apply(number: Long) = {
    // the top nybble is left alone (assumed to be 0) to keep things positive.
    // lowest 16 bits are moved to the top to spread out user_ids across 2^16 buckets.
    ((number << (60 - 16)) & 0x0ffff00000000000L) | ((number >>> 16) & 0x00000fffffffffffL) | (number & 0xf000000000000000L)
  }
}
