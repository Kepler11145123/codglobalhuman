package com.bbva.datioamproduct.cddglobalmodel

import io.cucumber.junit.{Cucumber, CucumberOptions}
import org.junit.runner.RunWith

@RunWith(classOf[Cucumber])
@CucumberOptions(
    features = Array("classpath:features"),
    glue = Array("com.datio.spark.bdt.steps","com.bbva.datioamproduct.cddglobalmodel.steps"),
    strict = true,
    plugin = Array("pretty"))
class RunCukesTest
