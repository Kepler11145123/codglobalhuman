package com.bbva.datioamproduct.cddglobalmodel.process
import com.bbva.datioamproduct.cddglobalmodel.data.ParametryEcoInformation.{LAUNCHER, WRITE_TEMP, BOOLEAN_TRUE}
import com.bbva.datioamproduct.cddglobalmodel.data.{GenerateEcoInformation, GetDataProcessEcoInformation, ParametryEcoInformation}
import com.bbva.datioamproduct.utils.io.WriterWithDataproc
import com.datio.dataproc.sdk.api.context.RuntimeContext
import com.datio.dataproc.sdk.datiosparksession.DatioSparkSession
import com.typesafe.scalalogging.LazyLogging
import com.typesafe.config.Config
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

class EvaluateEcoInformation(runtimeContext: RuntimeContext) extends LazyLogging {
  def run: Int = {
    var exitCode = ParametryEcoInformation.EXIT_CODE_INITIAL
    val config = runtimeContext.getConfig.getConfig(LAUNCHER)
    val datioSparkSession = DatioSparkSession.getOrCreate()
    val spark = datioSparkSession.getSparkSession
    spark.sparkContext.setCheckpointDir(config.getString(WRITE_TEMP))

    try {
      val inputs = new GetDataProcessEcoInformation(spark, config).getInputs
      val dfEcoInformation = new GenerateEcoInformation(spark, config).generateEcoInformation(inputs)
      new WriterWithDataproc(config).apply(dfEcoInformation, ParametryEcoInformation.OUTPUT_ROUTE)
      deleteTmpPath(config.getString(WRITE_TEMP),spark,config)
      exitCode = ParametryEcoInformation.EXIT_CODE_SUCCESS
    } catch {
      case permissionException: org.apache.hadoop.security.AccessControlException =>
        permissionException.printStackTrace()
        exitCode = ParametryEcoInformation.EXIT_CODE_FAIL_PERMISSION
      case exception: Exception =>
        exception.printStackTrace()
        exitCode = ParametryEcoInformation.EXIT_CODE_FAIL_GENERAL
    }
    exitCode
  }

  def deleteTmpPath(path: String, spark: SparkSession, config : Config): Boolean = {
    val hadoopConfig = spark.sparkContext.hadoopConfiguration
    val hadoopFileSystem = FileSystem.get(hadoopConfig)
    val pathHdfsPartition = new Path(path)
    hadoopFileSystem.delete(pathHdfsPartition, BOOLEAN_TRUE)
  }
}
