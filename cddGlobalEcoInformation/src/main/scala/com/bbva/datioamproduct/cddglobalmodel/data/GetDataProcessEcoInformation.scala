package com.bbva.datioamproduct.cddglobalmodel.data

import com.bbva.co.csan.csancospkoutcsancddpreprocessingv2.data.GenerateAgileDocs
import com.bbva.datioamproduct.cddglobalmodel.data.ParametryEcoInformation._
import com.bbva.datioamproduct.utils.catalogs.{ParametersCDD, Taxonomy}
import com.bbva.datioamproduct.utils.io.{ReaderCalculatedDataproc, ReaderWithDataproc}
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import java.time.LocalDate

class GetDataProcessEcoInformation(val spark: SparkSession, config: Config) extends LazyLogging {

  val param: Map[String, Any] = new ParametersCDD(spark, config, MASTER).param

  def getInputs: Map[String, DataFrame] = {
    val inputs = Map(
      dfTaxonomy -> getTaxonomy,
      dfSegment -> getSegmentIFRS9,
      dfCustomer -> getInputsStandard(LIST_CUST_COLUMNS, CFG_CUST_PATH),
      dfInfCus -> getInputsKirby(LIST_INFCUS_COLUMNS, CFG_INFCUST_PATH),
      dfPayCapac -> getStandardControlDF(LIST_PAYCAPAC_COLUMNS, CFG_PAYCAPAC_PATH),
      dfSalesBase -> getInputsStandard(LIST_SALES_BASE_COLUMNS, CFG_SALES_PATH),
      dfHdape094 -> getInputsKirby(HDAPE094_LIST, CFG_HDAPE094_PATH),
      dfTasaCambio -> getInputsStandardTasa(CFG_TASA_PATH),
      dfEndeuda -> getInputsStandard(LIST_ENDEU, CFG_ENDEUDA),
      dfSectorization -> getInputsStandard(LIST_SECTORIZATION, CFG_SECTORIZATION)
    )
    inputs
  }

  def getInputsStandard(columnList: List[String], inputLevel: String): DataFrame = {
    val columns = columnList.map(columnName => trim(col(columnName)).as(columnName))
    new ReaderWithDataproc(spark, config).apply(inputLevel).select(columns: _*)
  }

  def getStandardControlDF(columnList: List[String], inputLevel: String): DataFrame = {
    val columns = columnList.map(columnName => trim(col(columnName)).as(columnName))
    try {
      new ReaderWithDataproc(spark, config).apply(inputLevel).select(columns: _*)
    }
    catch {
      case e: Exception =>
        logger.error(inputLevel, e)
        spark.createDataFrame(spark.sparkContext.emptyRDD[Row],
          StructType(columnList.map(columnName => StructField(columnName, StringType, BOOLEAN_TRUE)))).select(columns: _*)
    }
  }

  def getTaxonomy: DataFrame = {
    new Taxonomy(spark, config).getRelValues(Seq(C289), IN_TAX_COLUMN_LIST, MASTER)
  }

  def getSegmentIFRS9: DataFrame = {
    val param = new ParametersCDD(spark, config, MASTER).param
    val columns = LIST_SEGME_COLUMNS.map(columnName => trim(col(columnName)).as(columnName))
    val dfInput = new GenerateAgileDocs(spark, PARAM_EMPTY, PARAM_EMPTY).generateSegments(config).checkpoint()
    dfInput.select(columns: _*).select(col(IFRS9), concat(lit(param(G_ENTITY_ID).toString), lit(NUMBER_ZERO), col(CUSTOMER_ID)).as(CUSTOMER_ID)
    )
  }

  def getInputsKirby(columnList: List[String], inputLevel: String): DataFrame = {
    val columns = columnList.map(columnName => trim(col(columnName)).as(columnName))
    val dateMax = LocalDate.parse(config.getString(CFG_LAST_DAY_MONTH)).plusDays(DAY_MAX)
    new ReaderCalculatedDataproc(spark, config).lastDataLoadDateParquet(inputLevel, dateMax.toString)
      .select(columns: _*)
  }

  def getInputsStandardTasa(inputLevel: String): DataFrame = {
    new ReaderWithDataproc(spark, config).apply(inputLevel)
      .filter(col(CURRENCY_ID) === "EUR" && col(EXCHANGE_CURRENCY_TYPE) === param(TIPO_TASA_CAMBIO)
        && col(EXCHANGE_RATE_APPLY_ENTITY_ID) === param(G_ENTITY))
      .select(col(EXCHANGE_RATE_AMOUNT))
  }

}
