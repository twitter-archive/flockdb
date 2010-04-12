package com.twitter.flockdb.unit

import com.twitter.service.flock.ByteSwapper
import org.specs.Specification

object ByteSwapperSpec extends Specification {
  "ByteSwapper" should {
    "reverse the bytes" in {
      ByteSwapper(0xabcdL) mustEqual 0x0abcd00000000000L
      ByteSwapper(0x01234567fedcba98L) mustEqual 0x0ba981234567fedcL
      ByteSwapper(0x41234567fedcba98L) mustEqual 0x4ba981234567fedcL
    }
  }
}
