package com.bbva.datioamproduct.cddglobalmodel.data

import com.bbva.datioamproduct.cddglobalmodel.ContextProvider
import com.bbva.datioamproduct.utils.io.ReaderWithDataproc
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{Column, DataFrame}
import org.scalatest.{FlatSpec, Matchers}

class GenerateEcoInformationTest extends FlatSpec with Matchers with ContextProvider {

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
  val dfPerimeterFilterPath = "inputs.dfPerimeterFilterPath"
  val dfSegmentTaxonomyPath = "inputs.dfSegmentTaxonomyPath"
  val dfChangeRatePath = "inputs.dfChangeRatePath"
  val dfPerimCltePath = "inputs.dfPerimCltePath"
  val dfEndeudaPath = "inputs.dfEndeudaPath"
  val dfInfCusPath = "inputs.dfInfCusPath"
  val dfEndeuProrityPath = "inputs.dfEndeuProrityPath"
  val dfGetTasaPath = "inputs.dfGetTasaPath"
  val dfInfoEndeuPath = "inputs.dfInfoEndeuPath"
  val dfJoinCustoPath = "inputs.dfJoinCustoPath"
  val dfSectorizationPath = "inputs.dfSectorizationPath"
  val dfJoinTypePath = "inputs.dfJoinTypePath"
  val dfGetTypePath = "inputs.dfGetTypePath"
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
      |      dfPerimeterFilterPath{
      |         type = parquet
      |         path = "src/test/resources/data/inputsTmp/dfPerimeterFilter"
      |      }
      |      dfSegmentTaxonomyPath{
      |         type = parquet
      |         path = "src/test/resources/data/inputsTmp/dfSegmentTaxonomy"
      |      }
      |      dfChangeRatePath{
      |         type = parquet
      |         path = "src/test/resources/data/inputsTmp/dfChangeRate"
      |      }
      |      dfPerimCltePath{
      |         type = parquet
      |         path = "src/test/resources/data/inputsTmp/dfPerimClte"
      |      }
      |      dfEndeudaPath{
      |         type = parquet
      |         path = "src/test/resources/data/inputsTmp/dfEndeuda"
      |      }
      |      dfInfCusPath{
      |         type = parquet
      |         path = "src/test/resources/data/inputsTmp/dfInfCus"
      |      }
      |      dfEndeuProrityPath{
      |         type = parquet
      |         path = "src/test/resources/data/inputsTmp/dfEndeuPrority"
      |      }
      |      dfGetTasaPath{
      |         type = parquet
      |         path = "src/test/resources/data/inputsTmp/dfGetTasa"
      |      }
      |      dfInfoEndeuPath{
      |         type = parquet
      |         path = "src/test/resources/data/inputsTmp/dfInfoEndeu"
      |      }
      |      dfJoinCustoPath{
      |         type = parquet
      |         path = "src/test/resources/data/inputsTmp/dfJoinCusto"
      |      }
      |      dfSectorizationPath{
      |         type = parquet
      |         path = "src/test/resources/data/inputsTmp/dfSectorization"
      |      }
      |      dfJoinTypePath{
      |         type = parquet
      |         path = "src/test/resources/data/inputsTmp/dfJoinType"
      |      }
      |      dfGetTypePath{
      |         type = parquet
      |         path = "src/test/resources/data/inputsTmp/dfGetType"
      |      }
      |   }
      |}
      |""".stripMargin
  val configccddEcoInformation: Config = ConfigFactory.parseString(configStringcddEcoInformation).getConfig("cddEcoInformation")

  "1. The test of GenerateEcoInformation.generateEcoInformation" should "be correct, obtain a object type dataframe with 14 columns and 15 records" in {
    spark.sparkContext.setCheckpointDir(config.getString(ParametryEcoInformation.WRITE_TEMP_DELETE))
    val inputs = new GetDataProcessEcoInformation(spark, config).getInputs
    val evaluate = new GenerateEcoInformation(spark, config).generateEcoInformation(inputs)
    assert(evaluate.isInstanceOf[DataFrame], "the object is not a dataframe")
    assert(evaluate.schema.fieldNames.length == 14, "wrong number of columns")
    assert(evaluate.count() == 15, "wrong number of records")
  }

  "2. When read the function createPerimeter  " should "return a Dataframe with 16 rows and 4 columns" in {
    val reader = new ReaderWithDataproc(spark, configccddEcoInformation)
    val dfCustomer = reader.apply(customerPath)
    val dfInfCus = reader.apply(infoCusPath)
    val evaluate = new GenerateEcoInformation(spark, config).createPerimeter(dfInfCus, dfCustomer)
    assert(evaluate.isInstanceOf[DataFrame] && evaluate.count() === 16 && evaluate.columns.length === 4)
  }

  "3. When read the function getPerimeterWithHdape094 " should "return a Dataframe with 16 rows and 7 columns" in {
    val reader = new ReaderWithDataproc(spark, configccddEcoInformation)
    val dfPerimeter = reader.apply(dfPerimeterPath)
    val dfHdape094 = reader.apply(dfHdape094Path)
    val evaluate = new GenerateEcoInformation(spark, config).getPerimeterWithHdape094(dfPerimeter, dfHdape094)
    assert(evaluate.isInstanceOf[DataFrame] && evaluate.count() === 16 && evaluate.columns.length === 7)
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

  "6. When read the function joinPerimClte" should " return a dataframe with 15 rows and 13 columns " in {
    val reader = new ReaderWithDataproc(spark, configccddEcoInformation)
    val dfJoinTax = reader.apply(dfJoinTaxPath)
    val dfAggClte = reader.apply(dfAggCltePath)
    val evaluate = new GenerateEcoInformation(spark, config).joinPerimClte(dfJoinTax, dfAggClte)
    assert(evaluate.count() == 15, " number of records")
    assert(evaluate.schema.length == 13)
  }

  "7. When read the function getPerimeterWithTaxoR019" should " return a dataframe with 23 rows and 8 columns " in {
    val reader = new ReaderWithDataproc(spark, configccddEcoInformation)
    val dfPerimeterFilter = reader.apply(dfPerimeterFilterPath)
    val dfSegmentTaxonomy = reader.apply(dfSegmentTaxonomyPath)
    val evaluate = new GenerateEcoInformation(spark, config).getPerimeterWithTaxoR019(dfPerimeterFilter, dfSegmentTaxonomy)
    assert(evaluate.count() == 23, " number of records")
    assert(evaluate.schema.length == 8)
  }

  "8. When read the function getSalesBaseWithInformationCus " should "return a dataframe with 20 rows and 34 columns " in {
    val reader = new ReaderWithDataproc(spark, configccddEcoInformation)
    val dfsaleBase = reader.apply(dfsaleBasePath)
    val dfinfoCus = reader.apply(infoCusPath)
    val evaluate = new GenerateEcoInformation(spark, config).getSalesBaseWithInformationCus(dfsaleBase, dfinfoCus)
    assert(evaluate.count() == 20, " number of records")
    assert(evaluate.schema.length == 34)
  }

  "9. When read the function getSegmentTaxonmy" should " return a dataframe with 15 rows and 3 columns " in {
    val reader = new ReaderWithDataproc(spark, configccddEcoInformation)
    val dfSegments = reader.apply(dfSegmentsPath)
    val dftaxR019 = reader.apply(taxR019Path)
    val evaluate = new GenerateEcoInformation(spark, config).getSegmentTaxonmy(dfSegments, dftaxR019)
    assert(evaluate.count() == 15, " number of records")
    assert(evaluate.schema.length == 3)
  }

  "10. When read the function getRateChange" should "return a dataframe with 49 rows and 15 columns" in {
    val reader = new ReaderWithDataproc(spark, configccddEcoInformation)
    val dfChangeRate = reader.apply(dfChangeRatePath)
    val dfPerimClte = reader.apply(dfPerimCltePath)
    val evaluate = new GenerateEcoInformation(spark, config).getRateChange(dfPerimClte, dfChangeRate)
    assert(evaluate.count == 49 && evaluate.columns.length == 15)
  }

  "11. When read the function getFilterPriorityENDEU" should "return a dataframe with 12 rows and 4 columns" in {
    val reader = new ReaderWithDataproc(spark, configccddEcoInformation)
    val dfEndeuda = reader.apply(dfEndeudaPath)
    val evaluate = new GenerateEcoInformation(spark, config).getFilterPriorityENDEU(dfEndeuda)
    assert(evaluate.count == 12 && evaluate.columns.length == 4)
  }

  "12. When read the function joinInfoendeu" should "return a dataframe with 9 rows and 3 columns" in {
    val reader = new ReaderWithDataproc(spark, configccddEcoInformation)
    val dfInfCus = reader.apply(dfInfCusPath)
    val dfEndeuPrority = reader.apply(dfEndeuProrityPath)
    val evaluate = new GenerateEcoInformation(spark, config).joinInfoendeu(dfInfCus, dfEndeuPrority)
    assert(evaluate.count == 9 && evaluate.columns.length == 3)
  }

  "13. When read the function joinCust" should "return a dataframe with 9 rows and 5 columns" in {
    val reader = new ReaderWithDataproc(spark, configccddEcoInformation)
    val dfInfoEndeu = reader.apply(dfInfoEndeuPath)
    val dfSectorization = reader.apply(dfSectorizationPath)
    val evaluate = new GenerateEcoInformation(spark, config).joinCust(dfInfoEndeu, dfSectorization)
    assert(evaluate.count == 9 && evaluate.columns.length == 5)
  }

  "14. When read the function joinTypeSize" should "return a dataframe with 97 rows and 17 columns" in {
    val reader = new ReaderWithDataproc(spark, configccddEcoInformation)
    val dfjoinCusto = reader.apply(dfJoinCustoPath)
    val clumnNullJointype = new GenerateEcoInformation(spark, config).applyConditionsNull
    val clumnFirstJointype = new GenerateEcoInformation(spark, config).applyConditionsPart1
    val clumnSecondJointype = new GenerateEcoInformation(spark, config).applyConditionsPart2
    val dfSectorization = reader.apply(dfSectorizationPath)
    val evaluate = new GenerateEcoInformation(spark, config).joinTypeSize(dfjoinCusto, dfSectorization,clumnNullJointype, clumnFirstJointype ,clumnSecondJointype )
    assert(evaluate.count == 97 && evaluate.columns.length == 17)
  }

  "15. When read applyConditionsNull" should "get a column" in {
    val reader = new ReaderWithDataproc(spark, configccddEcoInformation)
    val evaluate = new GenerateEcoInformation(spark, config).applyConditionsNull
    assert(evaluate.isInstanceOf[Column])
  }

  "16. When read applyConditionsPart1" should "get a column" in {
    val reader = new ReaderWithDataproc(spark, configccddEcoInformation)
    val dfJoinType = reader.apply(dfJoinTypePath)
    val evaluate = new GenerateEcoInformation(spark, config).applyConditionsPart1
    assert(evaluate.isInstanceOf[Column])
  }

  "17. When read applyConditionsPart2" should "get a column" in {
    val reader = new ReaderWithDataproc(spark, configccddEcoInformation)
    val dfJoinType = reader.apply(dfJoinTypePath)
    val evaluate = new GenerateEcoInformation(spark, config).applyConditionsPart2
    assert(evaluate.isInstanceOf[Column])
  }

  "18. When read the function getFilterPrioritySIZE" should "return a dataframe with 15 rows and 14 columns" in {
    val reader = new ReaderWithDataproc(spark, configccddEcoInformation)
    val dfGetType = reader.apply(dfGetTypePath)
    val evaluate = new GenerateEcoInformation(spark, config).getFilterPrioritySIZE(dfGetType)
    assert(evaluate.count == 15 && evaluate.columns.length == 14)
  }
}
