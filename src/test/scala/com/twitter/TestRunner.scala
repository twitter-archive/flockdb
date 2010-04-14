package com.twitter.flockdb

import com.twitter.xrayspecs.XraySpecsRunner


object TestRunner extends XraySpecsRunner("src/test/scala/**/" +
  System.getProperty("test_phase", "(unit|integration)") + "/*.scala")
