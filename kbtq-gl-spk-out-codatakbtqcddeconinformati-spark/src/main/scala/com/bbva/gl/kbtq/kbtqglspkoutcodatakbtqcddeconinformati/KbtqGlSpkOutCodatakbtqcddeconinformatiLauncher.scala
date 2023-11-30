package com.bbva.gl.kbtq.kbtqglspkoutcodatakbtqcddeconinformati

import com.datio.dataproc.sdk.launcher.SparkLauncher
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
  * Temporal workaround to launch a mainClass instead a processId in Dataproc
  * Remove when new runtime is available
  */
object KbtqGlSpkOutCodatakbtqcddeconinformatiLauncher {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /**
    * Main for launch your implementation of SparkProcess
    * @param args the only needed argument is the path to the configuration file
    */
  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      logger.error("Parameter configuration file path is mandatory. Exiting...")
      System.exit(1000)
    }
    SparkLauncher.main(Array(args(0), "KbtqGlSpkOutCodatakbtqcddeconinformati"))
  }
}
