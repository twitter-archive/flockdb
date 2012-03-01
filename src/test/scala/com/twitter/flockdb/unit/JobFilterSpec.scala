package com.twitter.flockdb

import org.specs.Specification
import scala.collection.JavaConversions._

object JobFilterSpec extends Specification {

  "NoOpFilter" should {
    "always return true" in {
      val filter = NoOpFilter
      filter(1, 2, 3) mustEqual true
    }
  }

  "SetFilter" should {
    "return true when no filters are configured" in {
      val filter = new SetFilter(Set[String]())
      filter(1, 2, 3) mustEqual true
    }

    "apply filters" in {
      val filter = new SetFilter(Set("1:2:3", "4:5:*", "6:*:7", "8:*:*", "*:9:10", "*:11:*"))

      // Fully-specified filter.
      filter(1, 2, 3) mustEqual false
      filter(1, 2, 4) mustEqual true
      filter(1, 3, 4) mustEqual true
      filter(2, 1, 3) mustEqual true

      // Source and destination filter on any graph.
      filter(4, 5, 1) mustEqual false
      filter(4, 5, 2) mustEqual false
      filter(4, 1, 1) mustEqual true

      // Source filter on a specific graph.
      filter(6, 1, 7) mustEqual false
      filter(6, 2, 7) mustEqual false
      filter(6, 1, 8) mustEqual true

      // Source filter on any graph.
      filter(8, 1, 1) mustEqual false
      filter(8, 1, 2) mustEqual false
      filter(8, 2, 1) mustEqual false
      filter(8, 2, 2) mustEqual false
      filter(7, 1, 1) mustEqual true
      filter(7, 1, 2) mustEqual true
      filter(7, 2, 1) mustEqual true
      filter(7, 2, 2) mustEqual true

      // Destination filter on a specific graph.
      filter(1, 9, 10) mustEqual false
      filter(2, 9, 10) mustEqual false
      filter(1, 9, 1) mustEqual true

      // Destination filter on any graph.
      filter(1, 11, 1) mustEqual false
      filter(1, 11, 2) mustEqual false
      filter(2, 11, 1) mustEqual false
      filter(2, 11, 2) mustEqual false
      filter(1, 12, 1) mustEqual true
      filter(1, 12, 2) mustEqual true
      filter(1, 12, 1) mustEqual true
      filter(1, 12, 2) mustEqual true
    }

    "not support filtering on all sources and destinations" in {
      // Invalid filter on all sources and destinations for a single graph.
      val filter = new SetFilter(Set("*:*:1"))
      filter(1, 1, 1) mustEqual true
      filter(1, 2, 1) mustEqual true
      filter(2, 1, 1) mustEqual true

      // Invalid filter on everything.
      val filter2 = new SetFilter(Set("*:*:*"))
      filter2(1, 1, 1) mustEqual true
      filter2(1, 2, 1) mustEqual true
      filter2(2, 1, 1) mustEqual true
      filter2(1, 1, 2) mustEqual true
      filter2(1, 2, 2) mustEqual true
      filter2(2, 1, 2) mustEqual true
    }

    "ignore invalid filters" in {
      val filter = new SetFilter(Set("!@#$%", "1:*", "*:2", "7:8:9"))
      filter(1, 2, 3) mustEqual true
      filter(3, 2, 1) mustEqual true
      filter(2, 2, 2) mustEqual true
      filter(1, 1, 1) mustEqual true
      filter(9, 8, 7) mustEqual true
      filter(7, 8, 9) mustEqual false
    }
  }
}
