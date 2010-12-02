package com.twitter.flockdb

import org.specs.Specification
import com.twitter.util.Eval
import java.io.File


object ConfigValidationSpec extends Specification {
  "Configuration Validation" should {
    "production_sjc.scala" >> {
      val config = Eval[flockdb.config.FlockDB](new File("config/production_common.scala"), new File("config/production_sjc.scala"))
      config mustNot beNull
    }

    "production_slc.scala" >> {
      val config = Eval[flockdb.config.FlockDB](new File("config/production_common.scala"), new File("config/production_slc.scala"))
      config mustNot beNull
    }
    "development.scala" >> {
      val config = Eval[flockdb.config.FlockDB](new File("config/development.scala"))
      config mustNot beNull
    }

    "test.scala" >> {
      val config = Eval[flockdb.config.FlockDB](new File("config/test.scala"))
      config mustNot beNull
    }
  }
}
