package com.bbva.gl.kbtq.kbtqglspkoutcodatakbtqcddeconinformati

import java.io.File
import java.net.URI
import java.nio.file.Paths

import com.datio.dataproc.sdk.api.context.RuntimeContext
import com.datio.dataproc.sdk.datiosparksession.DatioSparkSession
import com.datio.dataproc.sdk.launcher.process.config.ProcessConfigLoader
import com.datio.dataproc.sdk.schema.DatioSchema
import com.typesafe.config.Config
import org.apache.spark.sql.Row
import org.scalatest.{BeforeAndAfterEach, FunSuite, Matchers}

import scala.reflect.io.Directory

class KbtqGlSpkOutCodatakbtqcddeconinformatiTest extends FunSuite with Matchers with ContextProvider with BeforeAndAfterEach {

  private val outputPath = "target/data/result"

  override def afterEach(): Unit = {
    val directory = new Directory(new File(outputPath))
    directory.deleteRecursively()
  }

  test("should count total of medals by country") {

    // Given
    val config = new ProcessConfigLoader().fromPath("src/test/resources/config/application.conf")
    val runtimeContext = new FakeRuntimeContext(config)
    val jobConfig = config.getConfig("config")
    val output = jobConfig.getConfigList("outputs").get(0)
    val outputSchema = DatioSchema
      .getBuilder
      .fromURI(URI.create(output.getString("uri_schema")).normalize())
      .build()

    // When
    val exitCode = new KbtqGlSpkOutCodatakbtqcddeconinformati().runProcess(runtimeContext)

    // Then
    val datioSparkSession = DatioSparkSession.getOrCreate()
    val resultRow = datioSparkSession.read.option("delimiter", ",").datioSchema(outputSchema).csv(outputPath).collect().head
    exitCode shouldBe 0
    resultRow shouldBe Row("Afghanistan (AFG)", 4)
  }

  class FakeRuntimeContext(config: Config) extends RuntimeContext {

    override def getProcessId: String = "fake"

    override def getConfig: Config = config

    override def getProperty(propertyName: String): AnyRef = {
      throw new UnsupportedOperationException
    }

    override def setAdditionalInfo(additionalInfo: String): Unit = {}

    override def setMessage(message: String): Unit = {}

    override def setUserCode(userCode: String): Unit = {}
  }

}
