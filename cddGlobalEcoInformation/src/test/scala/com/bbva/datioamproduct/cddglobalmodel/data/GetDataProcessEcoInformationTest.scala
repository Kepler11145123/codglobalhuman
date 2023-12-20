package com.bbva.datioamproduct.cddglobalmodel.data

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.{FlatSpec, Matchers}
import com.bbva.datioamproduct.cddglobalmodel.ContextProvider

class GetDataProcessEcoInformationTest extends FlatSpec with Matchers with ContextProvider {

  val config: Config = ConfigFactory.load("config/applicationLocal.conf").getConfig("cddGlobalEcoInformation")

  "1. When read the function GetDataProcess with getInputsStandard" should "get a dataframe with at least 15 rows and 1 columns" in {
    spark.sparkContext.setCheckpointDir("src/test/resources/data/inputsTmp/segmentos")
    val proc = new GetDataProcessEcoInformation(spark, config)
    val df = proc.getInputsStandard(ParametryEcoInformation.LIST_CUST_COLUMNS,ParametryEcoInformation.CFG_CUST_PATH)
    assert(df.count() == 15, "number of records")
    assert(df.columns.length == 1, "number of columns")
  }

  "2. When read the function GetDataProcess with getInputsKirby" should "get a dataframe with at least 1000 rows and 2 columns" in {
    spark.sparkContext.setCheckpointDir("src/test/resources/data/inputsTmp/segmentos")
    val proc = new GetDataProcessEcoInformation(spark, config)
    val df = proc.getInputsKirby(ParametryEcoInformation.HDAPE094_LIST,ParametryEcoInformation.CFG_HDAPE094_PATH)
    assert(df.count() == 1000, "number of records")
    assert(df.columns.length == 2, "number of columns")
  }

  "3. When read the function GetDataProcess with getPayCapac" should "get a dataframe with at least 500 rows and 4 columns" in {
    spark.sparkContext.setCheckpointDir("src/test/resources/data/inputsTmp/segmentos")
    val proc = new GetDataProcessEcoInformation(spark, config)
    val df = proc.getStandardControlDF(ParametryEcoInformation.LIST_PAYCAPAC_COLUMNS,ParametryEcoInformation.CFG_PAYCAPAC_PATH)
    assert(df.count() == 500, "number of records")
    assert(df.columns.length == 4, "number of columns")
  }

  "4. When read the function GetDataProcess.getTaxonomy" should "get a dataframe with at least 8 rows and 2 columns" in {
    spark.sparkContext.setCheckpointDir("src/test/resources/data/inputsTmp/segmentos")
    val proc = new GetDataProcessEcoInformation(spark, config)
    val df = proc.getTaxonomy
    assert(df.count() == 8, "number of records")
    assert(df.columns.length == 2, "number of columns")
  }


  "5. When read the function GetDataProcess.getSegmentoFinRep" should "get a dataframe with " +
    "22 rows and 2 columns" in {
    spark.sparkContext.setCheckpointDir("src/test/resources/data/inputsTmp/segmentos")
    val proc = new GetDataProcessEcoInformation(spark, config)
    val dfReturn = proc.getSegmentIFRS9
    assert(dfReturn.count() === 22)
    assert(dfReturn.columns.length === 2)
  }

  "6. When read the function GetDataProcess.getInputs" should "get a dataframe with 9 row" in {
    spark.sparkContext.setCheckpointDir("src/test/resources/data/inputsTmp/segmentos")
    val proc = new GetDataProcessEcoInformation(spark, config)
    val rows = proc.getInputs.count(item => true)
    assert(rows == 9, "wrong number of records")
  }

  "7. When read the function GetDataProcess.getInputsParquet" should "get a dataframe with at least 10 row and 5 columns" in {
    spark.sparkContext.setCheckpointDir("src/test/resources/data/inputsTmp/segmentos")
    val proc = new GetDataProcessEcoInformation(spark, config)
    val df = proc.getInputsParquet(ParametryEcoInformation.LIST_DXACCOUNTLEVEL_COLUMNS, ParametryEcoInformation.DX_ACCOUNTLEVEL_PATH)
    assert(df.count() == 10, "number of records")
    assert(df.columns.length == 5, "number of columns")
  }
}
