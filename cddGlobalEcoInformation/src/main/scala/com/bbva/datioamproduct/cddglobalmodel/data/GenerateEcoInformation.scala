package com.bbva.datioamproduct.cddglobalmodel.data

import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import com.bbva.datioamproduct.cddglobalmodel.data.ParametryEcoInformation._
import com.bbva.datioamproduct.utils.catalogs.ParametersCDD

class GenerateEcoInformation(spark: SparkSession, config: Config) extends LazyLogging {

  val param: Map[String, Any] = new ParametersCDD(spark, config, MASTER).param

  def generateEcoInformation(inputs: Map[String, DataFrame]): DataFrame = {
    val dfGeneralAttributesFilter = getGeneralAttributesFilter(inputs(dfDxGeneralAtrb))
    val dfGeneralAtrbWithAccountLvlBySingleJoin = getGeneralAtrbWithAccountLvlBySingleJoin(dfGeneralAttributesFilter, inputs(dfDxAccountLevel))
    val dfGeneralAtrbWithAccountLvlJoin = getGeneralAtrbWithAccountLvlJoin(dfGeneralAtrbWithAccountLvlBySingleJoin, inputs(dfDxAccountLevel))
    val dfSalesBaseWithInformationCus = getSalesBaseWithInformationCus(inputs(dfSalesBase), inputs(dfInfCus))
    val dfPerimeterCustomer = createPerimeter(dfSalesBaseWithInformationCus, inputs(dfCustomer))
    val dfPerimeterWithHdape094 = getPerimeterWithHdape094(dfPerimeterCustomer, inputs(dfHdape094))
    val dfPerimeterUnn = getPerimeterUnn(dfPerimeterWithHdape094, dfGeneralAtrbWithAccountLvlJoin)
    val dfPerimeterFilter = getPerimeterFilter(dfPerimeterUnn)
    val dfSegmentTaxonomy = getSegmentTaxonmy(inputs(dfSegment), inputs(dfTaxonomy))
    val joinTax = getPerimeterWithTaxoR019(dfPerimeterFilter, dfSegmentTaxonomy)
    val join_clte = joinClte(inputs(dfInfCus), inputs(dfPayCapac))
    val agg_clte = aggClte(join_clte)
    joinPerimClte(joinTax, agg_clte)
  }

  def getGeneralAttributesFilter(dfGeneralAttributes: DataFrame): DataFrame = {
    val cutoff_date = config.getString(CFG_LAST_DAY_MONTH)
    val dfGeneralAttributesDateFil = dfGeneralAttributes.filter(col(GF_FFSS_START_DATE) <= cutoff_date && col(GF_FFSS_END_DATE) <= cutoff_date
      && col(G_FFSS_STATUS_TYPE).eqNullSafe(param(PAR_G_FFSS_STATUS_TYPE_3)))
    val rowNumberCol =
      Window.partitionBy(
        dfGeneralAttributesDateFil(G_CUSTOMER_ID))
        .orderBy(
          dfGeneralAttributesDateFil(G_CUSTOMER_ID).asc,
          dfGeneralAttributesDateFil(GF_FFSS_START_DATE).desc,
          dfGeneralAttributesDateFil(GF_FFSS_END_DATE).desc)
    dfGeneralAttributesDateFil
      .select(col(ALL_COLUMN_EXPR), row_number().over(rowNumberCol).as(PRIORITY))
      .filter(col(PRIORITY).equalTo(lit(NUMBER_ONE)))
      .drop(PRIORITY)
  }

  def getGeneralAtrbWithAccountLvlBySingleJoin(dfGeneralAttributesFilter: DataFrame, dfAccountLvl: DataFrame): DataFrame = {
    dfGeneralAttributesFilter.as(A).join(dfAccountLvl.as(B),
      col(A_POINT + G_CUSTOMER_ID) <=> col(B_POINT + G_CUSTOMER_ID), LEFT_JOIN).
      select(
        col(A_POINT + G_CUSTOMER_ID),
        col(A_POINT + GF_EMPLOYEES_NUMBER),
        when(col(B_POINT + GF_CUTOFF_DATE).isNotNull, col(B_POINT + GF_CUTOFF_DATE))
          .otherwise(param(PAR_GF_EMPLOYEES_NUMBER)).as(GF_BILLING_DATE),
        col(A_POINT + GF_FINANCIAL_STATEMENTS_ID)
      )
  }

  def getGeneralAtrbWithAccountLvlJoin(dfGeneralAtrbWithAccountLvlBySingleJoin: DataFrame, dfAccountLvl: DataFrame): DataFrame = {
    val parCrCuenta25Value = param(PAR_CR_CUENTA_25)
    val dfAccountLvlFilter = dfAccountLvl.filter(col(GF_BAL_SHEET_ACCOUNT_ID).isin(V_ASSET_ID, parCrCuenta25Value))
    dfGeneralAtrbWithAccountLvlBySingleJoin.as(A).join(dfAccountLvlFilter.as(B),
      col(A_POINT + G_CUSTOMER_ID) <=> col(B_POINT + G_CUSTOMER_ID) &&
        col(A_POINT + GF_FINANCIAL_STATEMENTS_ID) <=> col(B_POINT + GF_FINANCIAL_STATEMENTS_ID), LEFT_JOIN)
      .select(
        lit(param(G_ENTIFIC_ID)).cast(STRING_TYPE).as(G_ENTIFIC_ID),
        lit(config.getString(CFG_LAST_DAY_MONTH)).as(GF_CUTOFF_DATE),
        col(A_POINT + G_CUSTOMER_ID),
        when(col(B_POINT + GF_BAL_SHEET_ACCOUNT_ID) === V_ASSET_ID, col(GF_BAL_SHEET_ACCOUNT_AMOUNT))
          .otherwise(param(PAR_GF_TOTAL_ASSET_AMOUNT)).as(GF_TOTAL_ASSET_AMOUNT),
        when(col(B_POINT + GF_BAL_SHEET_ACCOUNT_ID) === parCrCuenta25Value, col(GF_BAL_SHEET_ACCOUNT_AMOUNT))
          .otherwise(param(PAR_GF_CUSTOMER_SALES_AMOUNT)).as(GF_CUSTOMER_SALES_AMOUNT),
        col(A_POINT + GF_EMPLOYEES_NUMBER),
        lit(param(PAR_GF_COMPANY_SIZE_DATE)).as(GF_COMPANY_SIZE_DATE),
        col(A_POINT + GF_BILLING_DATE),
        lit(NUMBER_TWO).as(SOURCE)
      )
  }

  def getSalesBaseWithInformationCus(dfSalesBase: DataFrame, dfInfCus: DataFrame): DataFrame = {
    dfSalesBase.as(A)
      .join(dfInfCus.as(B),
        col(A_POINT + PERSONAL_ID) === regexp_replace(col(B_POINT + PERSONAL_ID), REGEX_LEFT_ZERO, PARAM_EMPTY),
        INNER_JOIN)
      .select(
        col(A_POINT + ALL_COLUMN_EXPR),
        col(B_POINT + CUSTOMER_ID))
  }

  def createPerimeter(dfSalesBaseWithInformationCus: DataFrame, dfCustomer: DataFrame): DataFrame = {
    dfSalesBaseWithInformationCus.as(A)
      .join(dfCustomer.as(B),
        concat(lit(param(G_ENTITY_ID)), lit(NUMBER_ZERO), col(A_POINT + CUSTOMER_ID)) === col(B_POINT + G_CUSTOMER_ID), INNER_JOIN)
  }

  def getPerimeterWithHdape094(dfPerimeter: DataFrame, dfHdpape094: DataFrame): DataFrame = {

    dfPerimeter.as(A)
      .join(dfHdpape094.as(B), col(A_POINT + CUSTOMER_ID) === col(B_POINT + CUSTOMER_ID), LEFT_JOIN)
      .select(
        lit(param(G_ENTIFIC_ID)).cast(STRING_TYPE).as(G_ENTIFIC_ID),
        lit(config.getString(CFG_LAST_DAY_MONTH)).cast(DATE_TYPE).as(GF_CUTOFF_DATE),
        col(A_POINT + G_CUSTOMER_ID),
        col(FFSS_TOTAL_ASSET_AMOUNT).cast(DECIMAL_TYPE_26_6).as(GF_TOTAL_ASSET_AMOUNT),
        col(TOTAL_NET_ANNUAL_SALES_AMOUNT).cast(DECIMAL_TYPE_26_6).as(GF_CUSTOMER_SALES_AMOUNT),
        when(col(B_POINT + EMPLOYEES_NUMBER).isNull, lit(param(GF_EMPLOYEES_NUMBER)))
          .otherwise(col(B_POINT + EMPLOYEES_NUMBER)).cast(DECIMAL_TYPE_17).as(GF_EMPLOYEES_NUMBER),
        to_date(col(A_POINT + FINANCIAL_STATEMENTS_DATE), DATE_FORMAT).cast(DATE_TYPE).as(GF_COMPANY_SIZE_DATE),
        to_date(col(A_POINT + FINANCIAL_STATEMENTS_DATE), DATE_FORMAT).cast(DATE_TYPE).as(GF_BILLING_DATE),
        lit(NUMBER_ONE).as(SOURCE)
      )
  }

  def getPerimeterUnn(dfPerimeterWithHdape094: DataFrame, dfGeneralAtrbWithAccountLvlJoin: DataFrame): DataFrame = {
    dfPerimeterWithHdape094
      .union(dfGeneralAtrbWithAccountLvlJoin)
  }

  def getPerimeterFilter(dfGeneralAtrbWithAccountLvlUnn: DataFrame): DataFrame = {
    val rowNumberCol = Window.partitionBy(G_CUSTOMER_ID).orderBy(SOURCE)

    dfGeneralAtrbWithAccountLvlUnn
      .select(col(ALL_COLUMN_EXPR), row_number().over(rowNumberCol).as(PRIORITY))
      .filter(col(PRIORITY).equalTo(lit(NUMBER_ONE)))
      .drop(PRIORITY, SOURCE)
  }

  def getSegmentTaxonmy(dfSegment: DataFrame, dfTaxonomy: DataFrame): DataFrame = {
    dfSegment.as(A)
      .join(broadcast(dfTaxonomy).as(B), col(A_POINT + IFRS9) === col(B_POINT + GF_INITIAL_CATALOG_VAL_ID), LEFT_JOIN)
      .drop(GF_INITIAL_CATALOG_VAL_ID)
  }

  def getPerimeterWithTaxoR019(dfPerimeter: DataFrame, dfSegmentTaxonomy: DataFrame): DataFrame = {
    dfPerimeter.as(A)
      .join(dfSegmentTaxonomy.as(B), col(A_POINT + G_CUSTOMER_ID) === col(B_POINT + CUSTOMER_ID), LEFT_JOIN)
      .select(
        col(A_POINT + ALL_COLUMN_EXPR),
        when(col(B_POINT + GF_FINAL_CATALOG_VAL_ID).isNull, lit(param(G_COMPANY_SIZE_TYPE)))
          .otherwise(col(B_POINT + GF_FINAL_CATALOG_VAL_ID)).as(G_COMPANY_SIZE_TYPE))
  }

  def joinClte(dfInfoBasic: DataFrame, dfPayCapac: DataFrame): DataFrame = {
    dfInfoBasic.as(AS_IBC).join(dfPayCapac.as(AS_PCO),
      regexp_replace(col(AS_IBC + DOT + PERSONAL_TYPE), REGEX_LEFT_ZERO, PARAM_EMPTY) ===
        regexp_replace(col(AS_PCO + DOT + PERSONAL_TYPE), REGEX_LEFT_ZERO, PARAM_EMPTY) &&
        regexp_replace(col(AS_IBC + DOT + PERSONAL_ID), REGEX_LEFT_ZERO, PARAM_EMPTY) ===
          regexp_replace(col(AS_PCO + DOT + PERSONAL_ID), REGEX_LEFT_ZERO, PARAM_EMPTY), INNER_JOIN)
      .select(
        col(CUSTOMER_ID).as(CUSTOMER_ID),
        col(PROPOSAL_ID).as(PROPOSAL_ID),
        col(SEC_VRDT_LT_PMT_CAP_AMOUNT).as(SEC_VRDT_LT_PMT_CAP_AMOUNT)
      )
  }

  def aggClte(dfJoinClte: DataFrame): DataFrame = {
    val groupWindow = Window.partitionBy(CUSTOMER_ID).orderBy(desc(PROPOSAL_ID))
    dfJoinClte
      .select(
        col(CUSTOMER_ID),
        col(SEC_VRDT_LT_PMT_CAP_AMOUNT),
        row_number().over(groupWindow).as(AS_NUM))
      .filter(
        col(AS_NUM).equalTo(lit(NUMBER_ONE)))
      .select(
        col(CUSTOMER_ID),
        col(SEC_VRDT_LT_PMT_CAP_AMOUNT))
  }

  def joinPerimClte(dfJoinTax: DataFrame, dfAggClte: DataFrame): DataFrame = {
    dfJoinTax.as(TAX).join(dfAggClte.as(AS_CLTE),
      col(TAX + DOT + G_CUSTOMER_ID) === concat(lit(param(G_ENTITY_ID)), lit(NUMBER_ZERO), col(AS_CLTE + DOT + CUSTOMER_ID)), LEFT_JOIN)
      .select(
        col(GF_CUTOFF_DATE).cast(DATE_TYPE).as(GF_CUTOFF_DATE),
        col(G_CUSTOMER_ID).as(G_CUSTOMER_ID),
        col(GF_TOTAL_ASSET_AMOUNT).cast(DECIMAL_TYPE_26_6).as(GF_TOTAL_ASSET_AMOUNT),
        col(GF_CUSTOMER_SALES_AMOUNT).cast(DECIMAL_TYPE_26_6).as(GF_CUSTOMER_SALES_AMOUNT),
        col(GF_EMPLOYEES_NUMBER).cast(DECIMAL_TYPE_17).as(GF_EMPLOYEES_NUMBER),
        col(G_COMPANY_SIZE_TYPE).as(G_COMPANY_SIZE_TYPE),
        col(GF_COMPANY_SIZE_DATE).cast(DATE_TYPE).as(GF_COMPANY_SIZE_DATE),
        col(GF_BILLING_DATE).cast(DATE_TYPE).as(GF_BILLING_DATE),
        col(G_ENTIFIC_ID).as(G_ENTIFIC_ID),
        date_format(current_timestamp(), TIME_STAMP_FORMAT).cast(TIMESTAMP_TYPE).as(gf_audit_date),
        col(SEC_VRDT_LT_PMT_CAP_AMOUNT).cast(DECIMAL_TYPE_26_6).as(GF_RTL_CUST_PMT_ABLTY_AMOUNT),
        lit(param(GF_RNTL_ICM_CRE_IN_EXP_PER)).cast(DECIMAL_TYPE_12_9).as(GF_RNTL_ICM_CRE_IN_EXP_PER),
        lit(param(PARAMETER_NULL)).cast(DECIMAL_TYPE_26_6).as(GF_CO_SIZE_CAL_TL_ASSET_AMOUNT),
        lit(param(PARAMETER_NULL)).cast(DECIMAL_TYPE_17).as(GF_CO_SIZE_CAL_EMPLYS_NUMBER)
      )
  }
}
