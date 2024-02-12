package com.bbva.datioamproduct.cddglobalmodel

import com.bbva.datioamproduct.utils.commons.FakeRuntimeContext
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.{FlatSpec, Matchers}

class CddGlobalEcoInformationTest extends FlatSpec with Matchers with ContextProvider {
  val config: Config = ConfigFactory.load("config/cddGlobalEcoInformation.conf")

  "1. An execution Launcher.runProcess with configuration file cddGlobalEcoInformation.conf " should "have response 0" in {
    spark.sparkContext.setCheckpointDir("src/test/resources/data/inputsTmp/segmentos")
    val runtimeContext = new FakeRuntimeContext(config)
    val returnCode = new CddGlobalEcoInformation().runProcess(runtimeContext)
    assert(returnCode === 0)
  }
}
