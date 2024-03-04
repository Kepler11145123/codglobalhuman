package com.bbva.datioamproduct.cddglobalmodel

import com.bbva.datioamproduct.cddglobalmodel.data.ParametryEcoInformation
import com.bbva.datioamproduct.cddglobalmodel.data.ParametryEcoInformation.{LAUNCHER, WRITE_TEMP}
import com.bbva.datioamproduct.cddglobalmodel.process.EvaluateEcoInformation
import com.datio.dataproc.sdk.api.SparkProcess
import com.datio.dataproc.sdk.api.context.RuntimeContext
import com.datio.dataproc.sdk.datiosparksession.DatioSparkSession


class CddGlobalEcoInformation extends SparkProcess {

  override def runProcess(runtimeContext: RuntimeContext): Int = {
    var exitCode = ParametryEcoInformation.EXIT_CODE_INITIAL
    val spark = DatioSparkSession.getOrCreate().getSparkSession
    val config = runtimeContext.getConfig
    spark.sparkContext.setCheckpointDir(config.getString(WRITE_TEMP))
    val evaluate = new EvaluateEcoInformation(runtimeContext)
    exitCode = evaluate.run
    exitCode
  }

  override def getProcessId: String = LAUNCHER
}
