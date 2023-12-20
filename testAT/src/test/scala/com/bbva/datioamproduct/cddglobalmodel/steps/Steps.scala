package com.bbva.datioamproduct.cddglobalmodel.steps

import com.datio.dataproc.sdk.datiosparksession.DatioSparkSession
import com.datio.spark.bdt.utils.Common
import com.datio.spark.bdt.utils.Constants.{DELIMITER, HEADER}
import com.typesafe.scalalogging.LazyLogging
import io.cucumber.scala.{EN, ScalaDsl}
import org.apache.spark.sql.DataFrame
import org.scalatest.Matchers

import scala.collection.mutable

class Steps extends ScalaDsl with EN with Matchers with LazyLogging{

  implicit val datioSpark: DatioSparkSession = DatioSparkSession.getOrCreate()
  val pathsMap = mutable.Map.empty[String, String]

  Given("""^a dataframe located at path (.*) with alias (\S+)$""") {
    (uri: String, alias: String) =>
      pathsMap.put(alias, uri)
  }

  When ("""^I read (\S+) as csv (without|with) headers and delimiter (.*)$""") {
    (dfName: String, header: String, delimiter: String) => {
      var df: DataFrame = datioSpark.getSparkSession.emptyDataFrame
      var headerOption = "false"
      header match {
        case "without" => {
          headerOption = "false"
        }
        case "with" => {
          headerOption = "true"
        }
      }
      df = datioSpark.getSparkSession.read
        .options(Map(DELIMITER -> delimiter, HEADER -> headerOption))
        .csv(pathsMap(dfName))
      Common.dataframeProviderMap.put(dfName, df)
    }
  }
}
