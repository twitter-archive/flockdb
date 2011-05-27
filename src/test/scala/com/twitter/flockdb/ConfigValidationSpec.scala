package com.twitter.flockdb

import org.specs.Specification
import com.twitter.util.Eval
import java.io.File
import com.twitter.flockdb


object ConfigValidationSpec extends Specification {
  "Configuration Validation" should {
    "production.scala" >> {
      val config = Eval[flockdb.config.FlockDB](new File("config/production.scala"))
      config.logging()
      config mustNot beNull
    }
    "development.scala" >> {
      val config = Eval[flockdb.config.FlockDB](new File("config/development.scala"))
      config.logging()
      config mustNot beNull
    }

    "test.scala" >> {
      val config = Eval[flockdb.config.FlockDB](new File("config/test.scala"))
      config.logging()
      config mustNot beNull
    }
  }
}
