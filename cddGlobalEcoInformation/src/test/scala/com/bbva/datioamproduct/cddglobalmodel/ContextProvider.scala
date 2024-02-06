package com.bbva.datioamproduct.cddglobalmodel

import com.datio.dataproc.sdk.datiosparksession.DatioSparkSession
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.scalatest.{BeforeAndAfterAll, Suite}

trait ContextProvider extends BeforeAndAfterAll{

  self: Suite =>

  @transient var spark : SparkSession = _
  @transient var sparkContext : SparkContext = _
  @transient var sqlContext : SQLContext = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    spark = SparkSession
      .builder()
      .appName("spark session")
      .master("local[*]")
      .getOrCreate()

    val datioSparkSession = DatioSparkSession.getOrCreate()
    spark = datioSparkSession.getSparkSession

    sparkContext = spark.sparkContext

    sqlContext = spark.sqlContext
  }

  override def afterAll(): Unit = {
    super.afterAll()

    if (spark != null) {
      spark.stop()
    }
  }
}
