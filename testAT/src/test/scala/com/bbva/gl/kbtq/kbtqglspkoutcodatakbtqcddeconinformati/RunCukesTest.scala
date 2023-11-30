package com.bbva.gl.kbtq.kbtqglspkoutcodatakbtqcddeconinformati

import io.cucumber.junit.{Cucumber, CucumberOptions}
import org.junit.runner.RunWith

@RunWith(classOf[Cucumber])
@CucumberOptions(
    features = Array("classpath:features"),
    glue = Array("com.bbva.gl.kbtq.kbtqglspkoutcodatakbtqcddeconinformati.steps"),
    strict = true,
    plugin = Array("pretty"))
class RunCukesTest
