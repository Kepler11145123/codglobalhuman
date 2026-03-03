package com.bbva.datioamproduct.cddglobalmodel.data

import com.bbva.datioamproduct.cddglobalmodel.ContextProvider
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.{FlatSpec, Matchers}

class GetDataProcessEcoHumanTest extends FlatSpec with Matchers with ContextProvider {

  val config: Config = ConfigFactory.load("config/cddGlobalEcoHuman.conf").getConfig("cddGlobalEcoHuman")

  "1. When read the function GetDataProcess with getInputsStandard" should "get a dataframe with at least 15 rows and 1 columns" in {
    spark.sparkContext.setCheckpointDir(config.getString(ParametryEcoHuman.WRITE_TEMP_DELETE))
    val proc = new GetDataProcessEcoHuman(spark, config)
    val df = proc.getInputsStandard(ParametryEcoHuman.LIST_CUST_COLUMNS, ParametryEcoHuman.CFG_CUST_PATH)
    assert(df.count() == 15, "number of records")
    assert(df.columns.length == 1, "number of columns")
  }

  "2. When read the function GetDataProcess with getInputsKirby" should "get a dataframe with at least 1000 rows and 2 columns" in {
    spark.sparkContext.setCheckpointDir(config.getString(ParametryEcoHuman.WRITE_TEMP_DELETE))
    val proc = new GetDataProcessEcoHuman(spark, config)
    val df = proc.getInputsKirby(ParametryEcoHuman.HDAPE094_LIST, ParametryEcoHuman.CFG_HDAPE094_PATH)
    assert(df.count() == 1000, "number of records")
    assert(df.columns.length == 2, "number of columns")
  }

  "3. When read the function GetDataProcess with getPayCapac" should "get a dataframe with at least 500 rows and 4 columns" in {
    spark.sparkContext.setCheckpointDir(config.getString(ParametryEcoHuman.WRITE_TEMP_DELETE))
    val proc = new GetDataProcessEcoHuman(spark, config)
    val df = proc.getStandardControlDF(ParametryEcoHuman.LIST_PAYCAPAC_COLUMNS, ParametryEcoHuman.CFG_PAYCAPAC_PATH)
    assert(df.count() == 500, "number of records")
    assert(df.columns.length == 4, "number of columns")
  }

  "4. When read the function GetDataProcess.getTaxonomy" should "get a dataframe with at least 8 rows and 2 columns" in {
    spark.sparkContext.setCheckpointDir(config.getString(ParametryEcoHuman.WRITE_TEMP_DELETE))
    val proc = new GetDataProcessEcoHuman(spark, config)
    val df = proc.getTaxonomy
    assert(df.count() == 8, "number of records")
    assert(df.columns.length == 2, "number of columns")
  }

  "5. When read the function GetDataProcess.getSegmentoFinRep" should "get a dataframe with 27 rows and 2 columns" in {
    spark.sparkContext.setCheckpointDir(config.getString(ParametryEcoHuman.WRITE_TEMP_DELETE))
    val proc = new GetDataProcessEcoHuman(spark, config)
    val dfReturn = proc.getSegmentIFRS9
    assert(dfReturn.count() === 27)
    assert(dfReturn.columns.length === 2)
  }

  "6. When read the function GetDataProcess.getInputs" should "get a dataframe with 10 row" in {
    spark.sparkContext.setCheckpointDir(config.getString(ParametryEcoHuman.WRITE_TEMP_DELETE))
    val proc = new GetDataProcessEcoHuman(spark, config)
    val rows = proc.getInputs.count(item => true)
    assert(rows == 10, "wrong number of records")
  }

  "7. When read the function getInputsStandardTasa" should "get a dataframe with 1 rows and 1 columns" in {
    val proc = new GetDataProcessEcoHuman(spark, config)
    val df = proc.getHdatc081
    assert(df.count === 1 && df.columns.length ===1 )
  }
}
