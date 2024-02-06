package com.bbva.datioamproduct.cddglobalmodel.data

import com.bbva.datioamproduct.cddglobalmodel.ContextProvider
import com.bbva.datioamproduct.utils.io.ReaderWithDataproc
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.DataFrame
import org.scalatest.{FlatSpec, Matchers}

class GenerateEcoInformationTest  extends FlatSpec with Matchers with ContextProvider {

  val config: Config = ConfigFactory.load("config/cddGlobalEcoInformation.conf").getConfig("cddGlobalEcoInformation")
  val dfSegmentsPath = "inputs.dfSegmentsPath"
  val dfsaleBasePath = "inputs.dfsaleBasePath"
  val infoCusPath = "inputs.infoCusPath"
  val customerPath = "inputs.customerPath"
  val taxR019Path = "inputs.taxR019Path"
  val dfAggCltePath = "inputs.dfAggCltePath"
  val dfHdape094Path = "inputs.dfHdape094Path"
  val dfJoinCltePath = "inputs.dfJoinCltePath"
  val dfJoinTaxPath = "inputs.dfJoinTaxPath"
  val dfPayCapacPath = "inputs.dfPayCapacPath"
  val dfPerimeterPath = "inputs.dfPerimeterPath"
  val dfPerimeterWithHdape094Path = "inputs.dfPerimeterWithHdape094Path"
  val dfPerimeterFilterPath = "inputs.dfPerimeterFilterPath"
  val dfSegmentTaxonomyPath = "inputs.dfSegmentTaxonomyPath"
  val dfGeneralAttributesFilterPath = "inputs.dfGeneralAttributesFilterPath"
  val dfGeneralAtrbWithAccountLvlBySingleJoinPath = "inputs.dfGeneralAtrbWithAccountLvlBySingleJoinPath"
  val dfAccountLevelPath ="inputs.dfAccountLevelPath"
  val dfPerimeterUnnPath = "inputs.dfPerimeterUnnPath"
  val t_dx42_ffss_general_atrb = "inputs.t_dx42_ffss_general_atrb"

  val configStringcddEcoInformation: String =
    """
      |cddEcoInformation {
      |   inputs {
      |       dfSegmentsPath{
      |         type = parquet
      |         path = "src/test/resources/data/inputsTmp/dfSegment"
      |       }
      |       dfsaleBasePath{
      |         type = parquet
      |         path = "src/test/resources/data/inputs/udapebaven_base_de_ventas"
      |      }
      |      infoCusPath {
      |         type = parquet
      |         path = "src/test/resources/data/inputsTmp/hdape001"
      |      }
      |      customerPath {
      |         type = parquet
      |         path = "src/test/resources/data/inputsTmp/customer"
      |      }
      |      taxR019Path {
      |         type = parquet
      |         path = "src/test/resources/data/inputsTmp/taxR019"
      |      }
      |       dfAggCltePath{
      |         type = parquet
      |         path = "src/test/resources/data/inputsTmp/dfAggClte"
      |      }
      |       dfHdape094Path{
      |         type = parquet
      |         path = "src/test/resources/data/inputsTmp/dfHdape094"
      |      }
      |      dfJoinCltePath{
      |         type = parquet
      |         path = "src/test/resources/data/inputsTmp/dfJoinClte"
      |      }
      |      dfJoinTaxPath{
      |         type = parquet
      |         path = "src/test/resources/data/inputsTmp/dfJoinTax"
      |      }
      |      dfPayCapacPath{
      |         type = parquet
      |         path = "src/test/resources/data/inputsTmp/dfPayCapac"
      |      }
      |      dfPerimeterPath{
      |         type = parquet
      |         path = "src/test/resources/data/inputsTmp/dfPerimeter"
      |      }
      |      dfPerimeterWithHdape094Path{
      |         type = parquet
      |         path = "src/test/resources/data/inputsTmp/dfPerimeterWithHdape094"
      |      }
      |      dfPerimeterFilterPath{
      |         type = parquet
      |         path = "src/test/resources/data/inputsTmp/dfPerimeterFilter"
      |      }
      |      dfSegmentTaxonomyPath{
      |         type = parquet
      |         path = "src/test/resources/data/inputsTmp/dfSegmentTaxonomy"
      |      }
      |      dfGeneralAttributesFilterPath{
      |         type = parquet
      |         path = "src/test/resources/data/inputsTmp/dfGeneralAttributesFilter"
      |      }
      |      dfGeneralAtrbWithAccountLvlBySingleJoinPath{
      |         type = parquet
      |         path = "src/test/resources/data/inputsTmp/dfGeneralAtrbWithAccountLvlBySingleJoin"
      |      }
      |      dfAccountLevelPath{
      |         type = parquet
      |         path = "src/test/resources/data/inputs/t_dx42_ffss_account_level"
      |         partitions= ["g_entific_id=CO"]
      |      }
      |      dfPerimeterUnnPath{
      |         type = parquet
      |         path = "src/test/resources/data/inputsTmp/dfPerimeterUnn"
      |      }
      |      t_dx42_ffss_general_atrb{
      |         type = parquet
      |         path = "src/test/resources/data/inputs/t_dx42_ffss_general_atrb"
      |      }
      |   }
      |}
      |""".stripMargin
  val configccddEcoInformation: Config = ConfigFactory.parseString(configStringcddEcoInformation).getConfig("cddEcoInformation")


  "1. The test of GenerateEcoInformation.generateEcoInformation" should "be correct, obtain a object type dataframe with 14 columns and 18 records" in {
    spark.sparkContext.setCheckpointDir("src/test/resources/data/inputsTmp/segmentos")
    val inputs = new GetDataProcessEcoInformation(spark,config).getInputs
    val evaluate = new GenerateEcoInformation(spark, config).generateEcoInformation(inputs)
    assert(evaluate.isInstanceOf[DataFrame],"the object is not a dataframe")
    assert(evaluate.schema.fieldNames.length == 14,"wrong number of columns" )
    assert(evaluate.count() == 18, "wrong number of records")
  }

  "2. When read the function createPerimeter  " should "return a Dataframe with 16 rows and 4 columns" in {
    val reader = new ReaderWithDataproc(spark, configccddEcoInformation)
    val dfCustomer= reader.apply(customerPath)
    val dfInfCus = reader.apply(infoCusPath)
    val evaluate = new GenerateEcoInformation(spark, config).createPerimeter(dfInfCus, dfCustomer)
    assert(evaluate.isInstanceOf[DataFrame] && evaluate.count() === 16 && evaluate.columns.length === 4)
  }

  "3. When read the function getPerimeterWithHdape094 " should "return a Dataframe with 16 rows and 9 columns" in {
    val reader = new ReaderWithDataproc(spark, configccddEcoInformation)
    val dfPerimeter = reader.apply(dfPerimeterPath)
    val dfHdape094 = reader.apply(dfHdape094Path)
    val evaluate = new GenerateEcoInformation(spark, config).getPerimeterWithHdape094(dfPerimeter, dfHdape094)
    assert(evaluate.isInstanceOf[DataFrame] && evaluate.count() === 16 && evaluate.columns.length === 9)
  }

  "4. When read the function joinClte" should " return a dataframe with 20 rows and 3 columns " in {
    val reader = new ReaderWithDataproc(spark, configccddEcoInformation)
    val dfInfoBasic = reader.apply(infoCusPath)
    val dfPayCapac = reader.apply(dfPayCapacPath)
    val evaluate = new GenerateEcoInformation(spark, config).joinClte(dfInfoBasic, dfPayCapac)
    assert(evaluate.count() == 20, " number of records")
    assert(evaluate.schema.length == 3)
  }

  "5. When read the function aggClte" should " return a dataframe with 17 rows and 2 columns " in {
    val reader = new ReaderWithDataproc(spark, configccddEcoInformation)
    val dfJoinClte = reader.apply(dfJoinCltePath)
    val evaluate = new GenerateEcoInformation(spark, config).aggClte(dfJoinClte)
    assert(evaluate.count() == 17, " number of records")
    assert(evaluate.schema.length == 2)
  }

  "6. When read the function joinPerimClte" should " return a dataframe with 15 rows and 14 columns " in {
    val reader = new ReaderWithDataproc(spark, configccddEcoInformation)
    val dfJoinTax = reader.apply(dfJoinTaxPath)
    val dfAggClte = reader.apply(dfAggCltePath)
    val evaluate = new GenerateEcoInformation(spark, config).joinPerimClte(dfJoinTax, dfAggClte)
    assert(evaluate.count() == 15, " number of records")
    assert(evaluate.schema.length == 14)
  }

  "7. When read the function getPerimeterWithTaxoR019" should " return a dataframe with 23 rows and 9 columns " in {
    val reader = new ReaderWithDataproc(spark, configccddEcoInformation)
    val dfPerimeterFilter = reader.apply(dfPerimeterFilterPath)
    val dfSegmentTaxonomy = reader.apply(dfSegmentTaxonomyPath)
    val evaluate = new GenerateEcoInformation(spark, config).getPerimeterWithTaxoR019(dfPerimeterFilter, dfSegmentTaxonomy)
    assert(evaluate.count() == 23, " number of records")
    assert(evaluate.schema.length == 9)
  }

  "8. When read the function getGeneralAttributesFilter " should "return a dataframe with 3 rows and 30 columns " in {
    val reader = new ReaderWithDataproc(spark, configccddEcoInformation)
    val dfGeneralAtrb = reader.apply(t_dx42_ffss_general_atrb)
    val evaluate = new GenerateEcoInformation(spark, config).getGeneralAttributesFilter(dfGeneralAtrb)
    assert(evaluate.count() == 3, " number of records")
    assert(evaluate.schema.length == 30)
  }

  "9. When read the function getGeneralAtrbWithAccountLvlJoin " should "return a dataframe with 3 rows and 9 columns " in {
    val reader = new ReaderWithDataproc(spark, configccddEcoInformation)
    val dfGeneralAtrbWithAccountLvlBySingleJoin = reader.apply(dfGeneralAtrbWithAccountLvlBySingleJoinPath)
    val dfAccountLevel = reader.apply(dfAccountLevelPath)
    val evaluate = new GenerateEcoInformation(spark, config).getGeneralAtrbWithAccountLvlJoin(dfGeneralAtrbWithAccountLvlBySingleJoin,dfAccountLevel)
    assert(evaluate.count() == 3, " number of records")
    assert(evaluate.schema.length == 9)
  }

  "10. When read the function getSalesBaseWithInformationCus " should "return a dataframe with 16 rows and 33 columns " in {
    val reader = new ReaderWithDataproc(spark, configccddEcoInformation)
    val dfsaleBase = reader.apply(dfsaleBasePath)
    val dfinfoCus = reader.apply(infoCusPath)
    val evaluate = new GenerateEcoInformation(spark, config).getSalesBaseWithInformationCus(dfsaleBase,dfinfoCus)
    assert(evaluate.count() == 16, " number of records")
    assert(evaluate.schema.length == 34)
  }

  "11. When read the function getPerimeterUnn " should "return a dataframe with 30 rows and 12 columns " in {
    val reader = new ReaderWithDataproc(spark, configccddEcoInformation)
    val dfJoinTax = reader.apply(dfJoinTaxPath)
    val dfJoinTax2 = reader.apply(dfJoinTaxPath)
    val evaluate = new GenerateEcoInformation(spark, config).getPerimeterUnn(dfJoinTax,dfJoinTax2)
    assert(evaluate.count() == 30, " number of records")
    assert(evaluate.schema.length == 12)
  }

  "12. When read the function getPerimeterFilter" should " return a dataframe with 23 rows and 8 columns " in {
    val reader = new ReaderWithDataproc(spark, configccddEcoInformation)
    val dfPerimeterUnn = reader.apply(dfPerimeterUnnPath)
    val evaluate = new GenerateEcoInformation(spark, config).getPerimeterFilter(dfPerimeterUnn)
    assert(evaluate.count() == 23, " number of records")
    assert(evaluate.schema.length == 8)
  }

  "13. When read the function getSegmentTaxonmy" should " return a dataframe with 15 rows and 3 columns " in {
    val reader = new ReaderWithDataproc(spark, configccddEcoInformation)
    val dfSegments = reader.apply(dfSegmentsPath)
    val dftaxR019 = reader.apply(taxR019Path)
    val evaluate = new GenerateEcoInformation(spark, config).getSegmentTaxonmy(dfSegments,dftaxR019)
    assert(evaluate.count() == 15, " number of records")
    assert(evaluate.schema.length == 3)
  }

  "14. When read the function getGeneralAtrbWithAccountLvlBySingleJoin" should " return a dataframe with 5 rows and 4 columns " in {
    val reader = new ReaderWithDataproc(spark, configccddEcoInformation)
    val dfGeneralAttributesFilter = reader.apply(dfGeneralAttributesFilterPath)
    val dfAccountLevel = reader.apply(dfAccountLevelPath)
    val evaluate = new GenerateEcoInformation(spark, config).getGeneralAtrbWithAccountLvlBySingleJoin(dfGeneralAttributesFilter, dfAccountLevel)
    assert(evaluate.count() == 5, " number of records")
    assert(evaluate.schema.length == 4)
  }
}
