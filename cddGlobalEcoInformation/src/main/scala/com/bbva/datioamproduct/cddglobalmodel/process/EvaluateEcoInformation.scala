package com.bbva.datioamproduct.cddglobalmodel.process

import com.bbva.datioamproduct.cddglobalmodel.data.ParametryEcoInformation._
import com.bbva.datioamproduct.cddglobalmodel.data.{GenerateEcoInformation, GetDataProcessEcoInformation, ParametryEcoInformation}
import com.bbva.datioamproduct.utils.io.WriterWithDataproc
import com.datio.dataproc.sdk.api.context.RuntimeContext
import com.datio.dataproc.sdk.datiosparksession.DatioSparkSession
import com.typesafe.scalalogging.LazyLogging
import com.datio.dataproc.sdk.compactor.DataprocSdkCompactorComponent
import com.datio.dataproc.sdk.datiofilesystem.DatioFileSystem

class EvaluateEcoInformation(runtimeContext: RuntimeContext) extends LazyLogging {
  def run: Int = {
    DataprocSdkCompactorComponent.reload()
    var exitCode = ParametryEcoInformation.EXIT_CODE_INITIAL
    val config = runtimeContext.getConfig.getConfig(LAUNCHER)
    try {
      val datioSparkSession = DatioSparkSession.getOrCreate()
      val spark = datioSparkSession.getSparkSession
      val inputs = new GetDataProcessEcoInformation(spark, config).getInputs
      val dfEcoInformation = new GenerateEcoInformation(spark, config).generateEcoInformation(inputs)
      new WriterWithDataproc(config).apply(dfEcoInformation, ParametryEcoInformation.OUTPUT_ROUTE)
      exitCode = ParametryEcoInformation.EXIT_CODE_SUCCESS
    } catch {
      case permissionException: org.apache.hadoop.security.AccessControlException =>
        permissionException.printStackTrace()
        exitCode = ParametryEcoInformation.EXIT_CODE_FAIL_PERMISSION
      case exception: Exception =>
        exception.printStackTrace()
        exitCode = ParametryEcoInformation.EXIT_CODE_FAIL_GENERAL
    }
    finally {
      DatioFileSystem.get().dropPartition(config.getString(WRITE_TEMP_DELETE))
    }
    exitCode
  }
}
