package com.bbva.datioamproduct.cddglobalmodel

import com.bbva.datioamproduct.utils.commons.FakeRuntimeContext
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.{FlatSpec, Matchers}

class CddGlobalEcoHumanTest extends FlatSpec with Matchers with ContextProvider {
  val config: Config = ConfigFactory.load("config/cddGlobalEcoHuman.conf")

  "1. An execution Launcher.runProcess with configuration file cddGlobalEcoHuman.conf " should "have response 0" in {
    val runtimeContext = new FakeRuntimeContext(config)
    val returnCode = new CddGlobalEcoHuman().runProcess(runtimeContext)
    assert(returnCode === 0)
  }
}
