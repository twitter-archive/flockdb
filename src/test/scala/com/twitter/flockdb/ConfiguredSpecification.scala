package com.twitter.flockdb

import net.lag.configgy.Configgy
import org.specs.Specification

abstract class ConfiguredSpecification extends Specification {
//  Configgy.configure("config/test.conf")
  lazy val config = Configgy.config
}

