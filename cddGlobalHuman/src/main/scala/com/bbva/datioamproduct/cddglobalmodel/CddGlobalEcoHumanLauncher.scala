package com.bbva.datioamproduct.cddglobalmodel

import com.bbva.datioamproduct.cddglobalmodel.data.ParametryEcoHuman._
import com.datio.dataproc.sdk.launcher.SparkLauncher
import com.typesafe.scalalogging.LazyLogging

object CddGlobalEcoHumanLauncher extends LazyLogging {
  def main(args: Array[String]): Unit = {
    if (args.length == NUMBER_ZERO) {
      logger.error(LOGGER_ERROR)
      System.exit(NUMBER_ONE_THOUSAND)
    }
    SparkLauncher.main(Array(args(NUMBER_ZERO), LAUNCHER))
    SparkLauncher.main(Array(args(NUMBER_ZERO), LAUNCHER))
  }
}
