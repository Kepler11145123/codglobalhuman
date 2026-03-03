package com.bbva.datioamproduct.cddglobalmodel.process

import com.bbva.datioamproduct.cddglobalmodel.data.ParametryEcoHuman._
import com.bbva.datioamproduct.cddglobalmodel.data.{GenerateEcoHuman, GetDataProcessEcoHuman, ParametryEcoHuman}
import com.bbva.datioamproduct.utils.io.WriterWithDataproc
import com.datio.dataproc.sdk.api.context.RuntimeContext
import com.datio.dataproc.sdk.datiosparksession.DatioSparkSession
import com.typesafe.scalalogging.LazyLogging
import com.datio.dataproc.sdk.compactor.DataprocSdkCompactorComponent
import com.datio.dataproc.sdk.datiofilesystem.DatioFileSystem

class EvaluateEcoInformation(runtimeContext: RuntimeContext) extends LazyLogging {
  def run: Int = {
    DataprocSdkCompactorComponent.reload()
    var exitCode = ParametryEcoHuman.EXIT_CODE_INITIAL
    val config = runtimeContext.getConfig.getConfig(LAUNCHER)
    try {
      val datioSparkSession = DatioSparkSession.getOrCreate()
      val spark = datioSparkSession.getSparkSession
      val inputs = new GetDataProcessEcoHuman(spark, config).getInputs
      val dfEcoInformation = new GenerateEcoHuman(spark, config).generateEcoInformation(inputs)
      new WriterWithDataproc(config).apply(dfEcoInformation, ParametryEcoHuman.OUTPUT_ROUTE)
      exitCode = ParametryEcoHuman.EXIT_CODE_SUCCESS
    } catch {
      case permissionException: org.apache.hadoop.security.AccessControlException =>
        permissionException.printStackTrace()
        exitCode = ParametryEcoHuman.EXIT_CODE_FAIL_PERMISSION
      case exception: Exception =>
        exception.printStackTrace()
        exitCode = ParametryEcoHuman.EXIT_CODE_FAIL_GENERAL
    }
    finally {
      DatioFileSystem.get().dropPartition(config.getString(WRITE_TEMP_DELETE))
    }
    exitCode
  }
}
