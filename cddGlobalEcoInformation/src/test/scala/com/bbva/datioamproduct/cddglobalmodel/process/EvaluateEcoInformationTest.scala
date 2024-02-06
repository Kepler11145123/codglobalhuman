package com.bbva.datioamproduct.cddglobalmodel.process

import com.bbva.datioamproduct.cddglobalmodel.ContextProvider
import com.bbva.datioamproduct.utils.commons.FakeRuntimeContext
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import org.scalatest.{FlatSpec, Matchers}

class EvaluateEcoInformationTest  extends FlatSpec with Matchers with ContextProvider {
  val oldConfig: Config = ConfigFactory.load("config/cddGlobalEcoInformation.conf")
  "1. When execute Evaluate.run with ideal case" should "have response 0" in {
    spark.sparkContext.setCheckpointDir("src/test/resources/data/inputsTmp/segmentos")
    val runtimeContext = new FakeRuntimeContext(oldConfig)
        val evaluate = new EvaluateEcoInformation(runtimeContext).run
    assert(evaluate == 0,"Execution ended with errors")
  }
  "2. When execute Evaluate.run with failed case" should "have response <> 0" in {
    val newConfig = oldConfig
      .withValue("cddGlobalEcoInformation.params.ODATE", ConfigValueFactory.fromAnyRef("2019-08-31"))
    spark.sparkContext.setCheckpointDir("src/test/resources/data/inputsTmp/segmentos")
    val runtimeContext = new FakeRuntimeContext(newConfig)
    val evaluate = new EvaluateEcoInformation(runtimeContext).run
    assert(evaluate != 0,"Execution ended with errors")
  }
}
