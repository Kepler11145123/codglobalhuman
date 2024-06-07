package com.bbva.datioamproduct.cddglobalmodel.data

import com.bbva.datioamproduct.cddglobalmodel.data.ParametryEcoInformation._
import com.bbva.datioamproduct.utils.catalogs.ParametersCDD
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

class GenerateEcoInformation(spark: SparkSession, config: Config) extends LazyLogging {

  val param: Map[String, Any] = new ParametersCDD(spark, config, MASTER).param

  def generateEcoInformation(inputs: Map[String, DataFrame]): DataFrame = {
    val dfSalesBaseWithInformationCus = getSalesBaseWithInformationCus(inputs(dfSalesBase), inputs(dfInfCus))
    val dfPerimeterCustomer = createPerimeter(dfSalesBaseWithInformationCus, inputs(dfCustomer))
    val dfPerimeterWithHdape094 = getPerimeterWithHdape094(dfPerimeterCustomer, inputs(dfHdape094))
    val dfSegmentTaxonomy = getSegmentTaxonmy(inputs(dfSegment), inputs(dfTaxonomy))
    val joinTax = getPerimeterWithTaxoR019(dfPerimeterWithHdape094, dfSegmentTaxonomy)
    val join_clte = joinClte(inputs(dfInfCus), inputs(dfPayCapac)).checkpoint()
    val agg_clte = aggClte(join_clte)
    val Perim_Clte = joinPerimClte(joinTax, agg_clte)
    val get_Tasa = getTasaCambio(Perim_Clte, inputs(dfTasaCambio))
    val EndeuPrority  = getFilterPriorityENDEU(inputs(dfEndeuda))
    val InfoEndeu = joinInfoendeu(inputs(dfInfCus), EndeuPrority)
    val joinCusto = joinCust(InfoEndeu, inputs(dfSectorization))
    val NullConditions = applyConditionsNull
    val firstCondition = applyConditionsPart1
    val secondCondition = applyConditionsPart2
    val joinType = joinTypeSize(get_Tasa, joinCusto, NullConditions, firstCondition, secondCondition)
    getFilterPrioritySIZE(joinType)
  }

  def getSalesBaseWithInformationCus(dfSalesBase: DataFrame, dfInfCus: DataFrame): DataFrame = {
    dfSalesBase.as(A).join(dfInfCus.as(B), col(A_POINT + PERSONAL_ID) === regexp_replace(col(B_POINT + PERSONAL_ID), REGEX_LEFT_ZERO, PARAM_EMPTY), INNER_JOIN)
      .select(col(A_POINT + ALL_COLUMN_EXPR), col(B_POINT + CUSTOMER_ID))
  }

  def createPerimeter(dfSalesBaseWithInformationCus: DataFrame, dfCustomer: DataFrame): DataFrame = {
    dfSalesBaseWithInformationCus.as(A)
      .join(dfCustomer.as(B), concat(lit(param(G_ENTITY_ID)), lit(NUMBER_ZERO), col(A_POINT + CUSTOMER_ID)) === col(B_POINT + G_CUSTOMER_ID), INNER_JOIN)
  }

  def getPerimeterWithHdape094(dfPerimeter: DataFrame, dfHdpape094: DataFrame): DataFrame = {
    dfPerimeter.as(A).join(dfHdpape094.as(B), col(A_POINT + CUSTOMER_ID) === col(B_POINT + CUSTOMER_ID), LEFT_JOIN)
      .select(lit(config.getString(CFG_LAST_DAY_MONTH)).cast(DATE_TYPE).as(GF_CUTOFF_DATE), col(A_POINT + G_CUSTOMER_ID),
        col(FFSS_TOTAL_ASSET_AMOUNT).cast(DECIMAL_TYPE_26_6).as(GF_TOTAL_ASSET_AMOUNT),
        col(TOTAL_NET_ANNUAL_SALES_AMOUNT).cast(DECIMAL_TYPE_26_6).as(GF_CUSTOMER_SALES_AMOUNT),
        when(col(B_POINT + EMPLOYEES_NUMBER).isNull, lit(param(GF_EMPLOYEES_NUMBER)))
          .otherwise(col(B_POINT + EMPLOYEES_NUMBER)).cast(DECIMAL_TYPE_17).as(GF_EMPLOYEES_NUMBER),
        to_date(col(A_POINT + FINANCIAL_STATEMENTS_DATE), DATE_FORMAT).cast(DATE_TYPE).as(GF_COMPANY_SIZE_DATE),
        to_date(col(A_POINT + FINANCIAL_STATEMENTS_DATE), DATE_FORMAT).cast(DATE_TYPE).as(GF_BILLING_DATE)
      )
  }

  def getSegmentTaxonmy(dfSegment: DataFrame, dfTaxonomy: DataFrame): DataFrame = {
    dfSegment.as(A).join(broadcast(dfTaxonomy).as(B), col(A_POINT + IFRS9) === col(B_POINT + GF_INITIAL_CATALOG_VAL_ID), LEFT_JOIN)
      .drop(GF_INITIAL_CATALOG_VAL_ID)
  }

  def getPerimeterWithTaxoR019(dfPerimeter: DataFrame, dfSegmentTaxonomy: DataFrame): DataFrame = {
    dfPerimeter.as(A).join(dfSegmentTaxonomy.as(B), col(A_POINT + G_CUSTOMER_ID) === col(B_POINT + CUSTOMER_ID), LEFT_JOIN)
      .select(col(A_POINT + ALL_COLUMN_EXPR))
  }

  def joinClte(dfInfoBasic: DataFrame, dfPayCapac: DataFrame): DataFrame = {
    dfInfoBasic.as(AS_IBC).join(dfPayCapac.as(AS_PCO),
      regexp_replace(col(AS_IBC + DOT + PERSONAL_TYPE), REGEX_LEFT_ZERO, PARAM_EMPTY) ===
        regexp_replace(col(AS_PCO + DOT + PERSONAL_TYPE), REGEX_LEFT_ZERO, PARAM_EMPTY) &&
        regexp_replace(col(AS_IBC + DOT + PERSONAL_ID), REGEX_LEFT_ZERO, PARAM_EMPTY) ===
          regexp_replace(col(AS_PCO + DOT + PERSONAL_ID), REGEX_LEFT_ZERO, PARAM_EMPTY), INNER_JOIN)
      .select(col(CUSTOMER_ID).as(CUSTOMER_ID), col(PROPOSAL_ID).as(PROPOSAL_ID), col(SEC_VRDT_LT_PMT_CAP_AMOUNT).as(SEC_VRDT_LT_PMT_CAP_AMOUNT)
      )
  }

  def aggClte(dfJoinClte: DataFrame): DataFrame = {
    val groupWindow = Window.partitionBy(CUSTOMER_ID).orderBy(desc(PROPOSAL_ID))
    dfJoinClte.select(col(CUSTOMER_ID), col(SEC_VRDT_LT_PMT_CAP_AMOUNT), row_number().over(groupWindow).as(AS_NUM)).filter(col(AS_NUM).equalTo(lit(NUMBER_ONE)))
      .select(col(CUSTOMER_ID), col(SEC_VRDT_LT_PMT_CAP_AMOUNT))
  }

  def joinPerimClte(dfJoinTax: DataFrame, dfAggClte: DataFrame): DataFrame = {
    dfJoinTax.as(TAX)
      .join(dfAggClte.as(AS_CLTE), col(TAX + DOT + G_CUSTOMER_ID) === concat(lit(param(G_ENTITY_ID)), lit(NUMBER_ZERO), col(AS_CLTE + DOT + CUSTOMER_ID)),
        LEFT_JOIN)
      .select(
        col(GF_CUTOFF_DATE).cast(DATE_TYPE).as(GF_CUTOFF_DATE),
        col(G_CUSTOMER_ID).as(G_CUSTOMER_ID),
        col(GF_TOTAL_ASSET_AMOUNT).cast(DECIMAL_TYPE_26_6).as(GF_TOTAL_ASSET_AMOUNT),
        col(GF_CUSTOMER_SALES_AMOUNT).cast(DECIMAL_TYPE_26_6).as(GF_CUSTOMER_SALES_AMOUNT),
        col(GF_EMPLOYEES_NUMBER).cast(DECIMAL_TYPE_17).as(GF_EMPLOYEES_NUMBER),
        col(GF_COMPANY_SIZE_DATE).cast(DATE_TYPE).as(GF_COMPANY_SIZE_DATE),
        col(GF_BILLING_DATE).cast(DATE_TYPE).as(GF_BILLING_DATE),
        lit(param(G_ENTIFIC_ID)).cast(STRING_TYPE).as(G_ENTIFIC_ID),
        date_format(current_timestamp(), TIME_STAMP_FORMAT).cast(TIMESTAMP_TYPE).as(GF_AUDIT_DATE),
        col(SEC_VRDT_LT_PMT_CAP_AMOUNT).cast(DECIMAL_TYPE_26_6).as(GF_RTL_CUST_PMT_ABLTY_AMOUNT),
        lit(param(GF_RNTL_ICM_CRE_IN_EXP_PER)).cast(DECIMAL_TYPE_12_9).as(GF_RNTL_ICM_CRE_IN_EXP_PER),
        lit(param(PARAMETER_NULL)).cast(DECIMAL_TYPE_26_6).as(GF_CO_SIZE_CAL_TL_ASSET_AMOUNT),
        lit(param(PARAMETER_NULL)).cast(DECIMAL_TYPE_17).as(GF_CO_SIZE_CAL_EMPLYS_NUMBER)
      )
  }

  def getTasaCambio(dfJoinPerim: DataFrame, dfTasaCambio: DataFrame): DataFrame = {
    val rate = dfTasaCambio.first().getDecimal(NUMBER_ZERO)
    dfJoinPerim.as(A).select(col(A_POINT + ALL_COLUMN_EXPR), (col(A_POINT + GF_CUSTOMER_SALES_AMOUNT) / rate).as(GF_CUSTOMER_SALES_AMOUNT_EUR),
      (col(A_POINT + GF_TOTAL_ASSET_AMOUNT) / rate).as(GF_TOTAL_ASSET_AMOUNT_EUR))
  }

  def getFilterPriorityENDEU(dfEndeuda: DataFrame): DataFrame = {
    val itemList = LIST_CLASSIFID.map(x => param(x))
    val window = Window.partitionBy(PERSONAL_TYPE, PERSONAL_ID, PERSONAL_VERIF_DIGIT_TYPE)
      .orderBy(col(CONTRACT_BRANCH_ID).desc, col(CONTRACT_PRODUCT_ID).desc, col(CONTRACT_SEQUENCE_ID).desc, col(GL_ACCOUNT_ID).desc)
    dfEndeuda.select(col(PERSONAL_TYPE), col(PERSONAL_ID), col(PORTFOLIO_TYPE),
      col(CUSTOMER_GROUP_CLASSIF_ID), row_number().over(window).as(ROW))
      .filter(col(ROW) === ONE && col(PORTFOLIO_TYPE) === param(PORTFOLIO_TYPE_2) &&
        !col(CUSTOMER_GROUP_CLASSIF_ID).isin(itemList: _*))
      .drop(col(ROW))
  }

  def joinInfoendeu(dfInfBas: DataFrame, priprityEndeu: DataFrame): DataFrame = {
    dfInfBas.as(A).join(priprityEndeu.as(B),
      regexp_replace(col(A_POINT + PERSONAL_ID), ZEROS, PARAM_EMPTY) === regexp_replace(col(B_POINT + PERSONAL_ID), ZEROS, PARAM_EMPTY) && regexp_replace(
        col(A_POINT + PERSONAL_TYPE), ZEROS, PARAM_EMPTY) === regexp_replace(col(B_POINT + PERSONAL_TYPE), ZEROS, PARAM_EMPTY), INNER_JOIN)
      .select(col(A_POINT + CUSTOMER_ID), col(B_POINT + PORTFOLIO_TYPE).as(PORTFOLIO_TYPE), col(B_POINT + CUSTOMER_GROUP_CLASSIF_ID)
      )
  }

  def joinCust(joinEndeu: DataFrame, dfSecto: DataFrame): DataFrame = {
    val dfSecto_filter = dfSecto.filter(col(G_ASSET_ALLOCATION_SECTOR_TYPE) =!= param(ASSET_ALLOCATION_SECTOR_TYPE_O))
    joinEndeu.as(A).join(dfSecto_filter.as(B), substring(col(A_POINT + CUSTOMER_ID), NUMBER_EIGHT_M, NUMBER_EIGHT)
      === substring(col(B_POINT + G_CUSTOMER_ID), NUMBER_EIGHT_M, NUMBER_EIGHT), INNER_JOIN)
      .select(col(A_POINT + ALL_COLUMN_EXPR), col(B_POINT + ALL_COLUMN_EXPR))
  }

  def applyConditionsNull: Column = {
    when((col(GF_EMPLOYEES_NUMBER).isNull || trim(col(GF_EMPLOYEES_NUMBER)) === PARAM_EMPTY) ||
      ((col(GF_CUSTOMER_SALES_AMOUNT_EUR).isNull || trim(col(GF_CUSTOMER_SALES_AMOUNT_EUR)) === PARAM_EMPTY) &&
        (col(GF_TOTAL_ASSET_AMOUNT_EUR).isNull || trim(col(GF_TOTAL_ASSET_AMOUNT_EUR)) === PARAM_EMPTY)),
      param(MARCA_EMPRESAGRANDE_FALTAINFO))}

  def applyConditionsPart1: Column = {
    when(
      (col(GF_EMPLOYEES_NUMBER) < param(MICROEMPREAS_EMPLEADOS)) &&
        (col(GF_CUSTOMER_SALES_AMOUNT_EUR) < param(MICROEMPRESA_IMPORTE) ||
          col(GF_TOTAL_ASSET_AMOUNT_EUR) < param(MICROEMPRESA_IMPORTE)), param(MARCA_MICROEMPRESA))
      .when(
        ((col(GF_EMPLOYEES_NUMBER) < param(PEQEMPRESA_EMPLEADOS)) &&
          (col(GF_CUSTOMER_SALES_AMOUNT_EUR) < param(PEQEMPRESA_IMPORTE) ||
            col(GF_TOTAL_ASSET_AMOUNT_EUR) < param(PEQEMPRESA_IMPORTE))) &&
          (col(GF_EMPLOYEES_NUMBER) >= param(MICROEMPREAS_EMPLEADOS) ||
            (col(GF_CUSTOMER_SALES_AMOUNT_EUR) >= param(MICROEMPRESA_IMPORTE) &&
            col(GF_TOTAL_ASSET_AMOUNT_EUR) >= param(MICROEMPRESA_IMPORTE))), param(MARCA_PEQEMPRESA))
  }

  def applyConditionsPart2: Column = {
    when(
      ((col(GF_EMPLOYEES_NUMBER) < param(MEDIANAEMPREAS_EMPLEADOS)) &&
        (col(GF_CUSTOMER_SALES_AMOUNT_EUR) < param(MEDIANAEMPRESA_VOLUMEN) ||
          col(GF_TOTAL_ASSET_AMOUNT_EUR) < param(MEDIANAEMPRESA_ACTIVOS))) &&
        (col(GF_EMPLOYEES_NUMBER) >= param(PEQEMPRESA_EMPLEADOS) ||
          (col(GF_CUSTOMER_SALES_AMOUNT_EUR) >= param(PEQEMPRESA_IMPORTE) &&
            col(GF_TOTAL_ASSET_AMOUNT_EUR) >= param(PEQEMPRESA_IMPORTE))), param(MARCA_MEDIANAEMPRESA))
      .when(
        (col(GF_EMPLOYEES_NUMBER) >= param(MEDIANAEMPREAS_EMPLEADOS)) ||
          (col(GF_CUSTOMER_SALES_AMOUNT_EUR) >= param(MEDIANAEMPRESA_VOLUMEN) &&
          col(GF_TOTAL_ASSET_AMOUNT_EUR) >= param(MEDIANAEMPRESA_ACTIVOS)), param(MARCA_EMPRESAGRANDE))
      .otherwise(param(MARCA_EMPRESAGRANDE_FALTAINFO))
  }

  def joinTypeSize(getTasa: DataFrame, dfJoinCus: DataFrame, NullCondition: Column, firstCondition: Column, secondCondition: Column): DataFrame = {
    val condition = coalesce(NullCondition, firstCondition, secondCondition)
    getTasa.as(A).join(dfJoinCus.as(B), substring(col(A_POINT + G_CUSTOMER_ID), NUMBER_EIGHT_M, NUMBER_EIGHT)
      === substring(col(B_POINT + G_CUSTOMER_ID), NUMBER_EIGHT_M, NUMBER_EIGHT), LEFT_JOIN)
      .select(col(A_POINT + ALL_COLUMN_EXPR),
        when(col(B_POINT + G_CUSTOMER_ID).isNotNull, condition)
          .otherwise(param(MARCA_EMPRESAGRANDE_FALTAINFO))
          .as(G_COMPANY_SIZE_TYPE))
      .drop(GF_CUSTOMER_SALES_AMOUNT_EUR, GF_TOTAL_ASSET_AMOUNT_EUR)
  }

  def getFilterPrioritySIZE(dfjoinTypeSize: DataFrame): DataFrame = {
    val window = Window.partitionBy(G_CUSTOMER_ID).orderBy(col(G_COMPANY_SIZE_TYPE).asc)
    dfjoinTypeSize.select(col(ALL_COLUMN_EXPR), row_number().over(window).as(ROW)).filter(col(ROW) === ONE)
      .drop(ROW)
  }
}
