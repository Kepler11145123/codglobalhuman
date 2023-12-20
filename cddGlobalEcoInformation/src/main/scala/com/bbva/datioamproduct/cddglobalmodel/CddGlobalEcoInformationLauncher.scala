package com.bbva.datioamproduct.cddglobalmodel

import com.bbva.datioamproduct.cddglobalmodel.data.ParametryEcoInformation.{LAUNCHER, NUMBER_ONE_THOUSAND, NUMBER_ZERO}
import com.datio.dataproc.sdk.launcher.SparkLauncher
import com.typesafe.scalalogging.LazyLogging

object CddGlobalEcoInformationLauncher extends LazyLogging {
  def main(args: Array[String]): Unit = {
    if (args.length == NUMBER_ZERO) {
      logger.error("Parameter configuration file path is mandatory. Exiting...")
      System.exit(NUMBER_ONE_THOUSAND)
    }
    SparkLauncher.main(Array(args(NUMBER_ZERO), LAUNCHER))
  }
}
