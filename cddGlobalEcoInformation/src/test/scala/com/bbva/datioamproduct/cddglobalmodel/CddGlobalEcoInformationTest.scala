package com.bbva.datioamproduct.cddglobalmodel

import com.bbva.datioamproduct.utils.commons.FakeRuntimeContext
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.{FlatSpec, Matchers}

class CddGlobalEcoInformationTest extends FlatSpec with Matchers with ContextProvider {
  val config: Config = ConfigFactory.load("config/applicationLocal.conf")

  "1. An execution Launcher.runProcess with configuration file applicationLocal.conf " should "have response 0" in {
    spark.sparkContext.setCheckpointDir("src/test/resources/data/inputsTmp/segmentos")
    val runtimeContext = new FakeRuntimeContext(config)
    val returnCode = new CddGlobalEcoInformation().runProcess(runtimeContext)
    assert(returnCode === 0)
  }

  "2. An execution Launcher.defineBusinessInfo " should "have response not null" in {
    val returnCode = new CddGlobalEcoInformation().defineBusinessInfo
    assert(returnCode != null)
  }
}
