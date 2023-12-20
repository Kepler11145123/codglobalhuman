package com.bbva.datioamproduct.cddglobalmodel

import com.bbva.datioamproduct.cddglobalmodel.data.ParametryEcoInformation
import com.bbva.datioamproduct.cddglobalmodel.data.ParametryEcoInformation.LAUNCHER
import com.bbva.datioamproduct.cddglobalmodel.process.EvaluateEcoInformation
import com.bbva.datioamproduct.utils.commons.BusinessInformation
import com.datio.dataproc.sdk.api.SparkProcess
import com.datio.dataproc.sdk.api.context.RuntimeContext
import org.slf4j.LoggerFactory


class CddGlobalEcoInformation extends SparkProcess {

  private val logger = LoggerFactory.getLogger(classOf[CddGlobalEcoInformation])

  override def runProcess(runtimeContext: RuntimeContext): Int = {
    var exitCode = ParametryEcoInformation.EXIT_CODE_INITIAL
    val evaluate = new EvaluateEcoInformation(runtimeContext)
    exitCode = evaluate.run
    exitCode

  }

  def defineBusinessInfo: BusinessInformation =
    BusinessInformation(exitCode = ParametryEcoInformation.EXIT_CODE_SUCCESS, entity = ParametryEcoInformation.PARAM_EMPTY,
      path = ParametryEcoInformation.PARAM_EMPTY, mode = ParametryEcoInformation.PARAM_EMPTY, schema = ParametryEcoInformation.PARAM_EMPTY,
      schemaVersion = ParametryEcoInformation.PARAM_EMPTY, reprocessing = ParametryEcoInformation.PARAM_EMPTY)

  override def getProcessId: String = LAUNCHER
}
