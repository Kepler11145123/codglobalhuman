package com.bbva.datioamproduct.cddglobalmodel

import com.bbva.datioamproduct.cddglobalmodel.data.ParametryEcoInformation
import com.bbva.datioamproduct.cddglobalmodel.data.ParametryEcoInformation.LAUNCHER
import com.bbva.datioamproduct.cddglobalmodel.process.EvaluateEcoInformation
import com.bbva.datioamproduct.utils.commons.BusinessInformation
import com.datio.dataproc.sdk.api.SparkProcess
import com.datio.dataproc.sdk.api.context.RuntimeContext
import org.slf4j.LoggerFactory


class CddGlobalEcoInformation extends SparkProcess {

  override def runProcess(runtimeContext: RuntimeContext): Int = {
    var exitCode = ParametryEcoInformation.EXIT_CODE_INITIAL
    val evaluate = new EvaluateEcoInformation(runtimeContext)
    exitCode = evaluate.run
    exitCode

  }

  override def getProcessId: String = LAUNCHER
}
