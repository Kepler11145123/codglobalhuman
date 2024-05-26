package com.bbva.datioamproduct.cddglobalmodel.process

import com.bbva.datioamproduct.cddglobalmodel.data.ParametryEcoInformation._
import com.bbva.datioamproduct.cddglobalmodel.data.{GenerateEcoInformation, GetDataProcessEcoInformation, ParametryEcoInformation}
import com.bbva.datioamproduct.utils.io.WriterWithDataproc
import com.datio.dataproc.sdk.api.context.RuntimeContext
import com.datio.dataproc.sdk.datiosparksession.DatioSparkSession
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import com.datio.dataproc.compactor.compaction.CompactorBuilder
import org.apache.hadoop.fs.{FileSystem}
import com.datio.dataproc.sdk.datiofilesystem.DatioFileSystem

class EvaluateEcoInformation(runtimeContext: RuntimeContext) extends LazyLogging {
  def run: Int = {
    var exitCode = ParametryEcoInformation.EXIT_CODE_INITIAL
    val config = runtimeContext.getConfig.getConfig(LAUNCHER)
    try {
      val datioSparkSession = DatioSparkSession.getOrCreate()
      val spark = datioSparkSession.getSparkSession
      val inputs = new GetDataProcessEcoInformation(spark, config).getInputs
      val dfEcoInformation = new GenerateEcoInformation(spark, config).generateEcoInformation(inputs)
      //new WriterWithDataproc(config).apply(dfEcoInformation, ParametryEcoInformation.OUTPUT_ROUTE)
      dfEcoInformation.write.partitionBy("g_entific_id", "gf_cutoff_date")
        .option("partitionOverwriteMode", "dynamic")
        .mode("overwrite").parquet(config.getString(ParametryEcoInformation.OUTPUT_ROUTE + ".path"))
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
      delete(config.getString(WRITE_TEMP_DELETE))
    }
    exitCode
  }

  def delete(path: String): Boolean = {
    val fileSystem: FileSystem = DatioFileSystem.get().qualify(path: String).fileSystem()
    val filePath = DatioFileSystem.get().qualify(path: String).path()
    fileSystem.delete(filePath, BOOLEAN_TRUE)
  }
}
