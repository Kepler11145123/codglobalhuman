package com.bbva.datioamproduct.cddglobalmodel.data

object ParametryEcoInformation {

  lazy val EXIT_CODE_INITIAL = 1
  lazy val EXIT_CODE_SUCCESS = 0
  lazy val EXIT_CODE_FAIL_PERMISSION: Int = -2
  lazy val EXIT_CODE_FAIL_GENERAL: Int = -1
  lazy val PARAM_EMPTY = ""
  lazy val TIME_STAMP_FORMAT = "yyyy-MM-dd HH:mm:ss.SSSSSS"
  lazy val NUMBER_ZERO = 0
  lazy val NUMBER_ONE = 1
  lazy val NUMBER_TWO = 2
  lazy val DATE_FORMAT = "yyyy-MM-dd"
  lazy val DECIMAL_TYPE_26_6 = "decimal(26,6)"
  lazy val DECIMAL_TYPE_12_9 = "decimal(12,9)"
  lazy val DECIMAL_TYPE_17 = "decimal(17)"
  lazy val DATE_TYPE = "date"
  lazy val TIMESTAMP_TYPE = "timestamp"
  lazy val STRING_TYPE = "String"
  lazy val CFG_CUST_PATH = "inputs.kbtq_eom_customer"
  lazy val CFG_INFCUST_PATH = "inputs.hdape001_info_basica_clte"
  lazy val CFG_PAYCAPAC_PATH = "inputs.hdarvpcocp_pco_capac_pago"
  lazy val DX_GENERALATRB_PATH = "inputs.dxgeneralatrb"
  lazy val DX_ACCOUNTLEVEL_PATH = "inputs.dxaccountlevel"
  lazy val CFG_LAST_DAY_MONTH = "params.last_day_month"
  lazy val OUTPUT_ROUTE = "output.economicalinformation"
  lazy val LAUNCHER = "cddGlobalEcoInformation"
  lazy val WRITE_TEMP = "WRITE_SEGMENTATION"
  lazy val GF_CUTOFF_DATE = "gf_cutoff_date"
  lazy val G_CUSTOMER_ID = "g_customer_id"
  lazy val GF_TOTAL_ASSET_AMOUNT = "gf_total_asset_amount"
  lazy val GF_CUSTOMER_SALES_AMOUNT = "gf_customer_sales_amount"
  lazy val GF_EMPLOYEES_NUMBER = "gf_employees_number"
  lazy val G_COMPANY_SIZE_TYPE = "g_company_size_type"
  lazy val GF_COMPANY_SIZE_DATE = "gf_company_size_date"
  lazy val GF_BILLING_DATE = "gf_billing_date"
  lazy val G_ENTIFIC_ID = "g_entific_id"
  lazy val gf_audit_date = "gf_audit_date"
  lazy val PERSONAL_ID = "personal_id"
  lazy val LIST_SEGME_COLUMNS: List[String] = List(CUSTOMER_ID, IFRS9)
  lazy val LIST_CUST_COLUMNS: List[String] = List(G_CUSTOMER_ID)
  lazy val LIST_INFCUS_COLUMNS: List[String] = List(PERSONAL_TYPE, PERSONAL_ID, CUSTOMER_ID)
  lazy val LIST_PAYCAPAC_COLUMNS: List[String] = List(PERSONAL_TYPE, PERSONAL_ID, PROPOSAL_ID, SEC_VRDT_LT_PMT_CAP_AMOUNT)
  lazy val EMPLOYEES_NUMBER = "employees_number"
  lazy val GF_INITIAL_CATALOG_VAL_ID = "gf_initial_catalog_val_id"
  lazy val GF_FINAL_CATALOG_VAL_ID = "gf_final_catalog_val_id"
  lazy val IN_TAX_COLUMN_LIST: Seq[String] = Seq(GF_INITIAL_CATALOG_VAL_ID, GF_FINAL_CATALOG_VAL_ID)
  lazy val PERSONAL_TYPE = "personal_type"
  lazy val CUSTOMER_ID = "customer_id"
  lazy val PROPOSAL_ID = "proposal_id"
  lazy val SEC_VRDT_LT_PMT_CAP_AMOUNT = "sec_vrdt_lt_pmt_cap_amount"
  lazy val FINANCIAL_STATEMENTS_DATE = "financial_statements_date"
  lazy val TOTAL_NET_ANNUAL_SALES_AMOUNT = "total_net_annual_sales_amount"
  lazy val FFSS_TOTAL_ASSET_AMOUNT = "ffss_total_asset_amount"
  lazy val BOOLEAN_TRUE = true
  lazy val IFRS9 = "IFRS9"
  lazy val AS_PCO = "pco"
  lazy val AS_IBC = "ibc"
  lazy val AS_NUM = "num"
  lazy val AS_CLTE = "clte"
  lazy val TAX = "tax"
  lazy val C289 = "C289"
  lazy val LEFT_JOIN = "left"
  lazy val INNER_JOIN ="inner"
  lazy val G_ENTITY_ID = "g_entity_id"
  lazy val dfTaxonomy = "dfTaxonomy"
  lazy val dfSegment = "dfSegment"
  lazy val dfCustomer = "dfCustomer"
  lazy val dfInfCus = "dfInfCus"
  lazy val dfPayCapac = "dfPayCapac"
  lazy val dfDxGeneralAtrb =  "dfDxGeneralAtrb"
  lazy val dfDxAccountLevel = "dfDxAccountLevel"
  lazy val REGEX_LEFT_ZERO = "^[0]*"
  lazy val DOT = "."
  lazy val GF_RTL_CUST_PMT_ABLTY_AMOUNT = "gf_rtl_cust_pmt_ablty_amount"
  lazy val GF_RNTL_ICM_CRE_IN_EXP_PER = "gf_rntl_icm_cre_in_exp_per"
  lazy val PARTITION_GF_CUTOFF_DATE = "gf_cutoff_date"
  lazy val LOGGER_ERROR = "Parameter configuration file path is mandatory. Exiting..."

  lazy val dfSalesBase = "dfSalesBase"
  lazy val CFG_SALES_PATH = "inputs.udapebaven_base_de_ventas"
  lazy val IN_SALES_BASE_PERSONAL_ID = "personal_id"
  lazy val LIST_SALES_BASE_COLUMNS: List[String] = List(
    PERSONAL_TYPE,
    IN_SALES_BASE_PERSONAL_ID,
    FFSS_TOTAL_ASSET_AMOUNT,
    TOTAL_NET_ANNUAL_SALES_AMOUNT,
    FINANCIAL_STATEMENTS_DATE
  )
  lazy val DAY_MAX = 5

  lazy val A = "A"
  lazy val B = "B"
  lazy val A_POINT = "A."
  lazy val B_POINT = "B."

  lazy val ALL_COLUMN_EXPR = "*"
  lazy val dfHdape094 = "dfHdape094"
  lazy val HDAPE094_LIST: List[String] = List(EMPLOYEES_NUMBER, CUSTOMER_ID)
  lazy val CFG_HDAPE094_PATH = "inputs.hdape094_info_mercadeo"
  lazy val NUMBER_ONE_THOUSAND = 1000
  lazy val MASTER = "master"
  lazy val GF_FFSS_START_DATE = "gf_ffss_start_date"
  lazy val GF_FFSS_END_DATE = "gf_ffss_end_date"
  lazy val G_FFSS_STATUS_TYPE = "g_ffss_status_type"
  lazy val GF_BAL_SHEET_ACCOUNT_ID = "gf_bal_sheet_account_id"
  lazy val GF_BAL_SHEET_ACCOUNT_AMOUNT = "gf_bal_sheet_account_amount"
  lazy val GF_FINANCIAL_STATEMENTS_ID = "gf_financial_statements_id"

  lazy val LIST_DXGENERALATRB_COLUMNS: List[String] = List(GF_FFSS_START_DATE,
    GF_FFSS_END_DATE, G_FFSS_STATUS_TYPE, G_CUSTOMER_ID, GF_EMPLOYEES_NUMBER, GF_FINANCIAL_STATEMENTS_ID)
  lazy val LIST_DXACCOUNTLEVEL_COLUMNS: List[String] = List(GF_CUTOFF_DATE,
    G_CUSTOMER_ID, GF_BAL_SHEET_ACCOUNT_ID, GF_BAL_SHEET_ACCOUNT_AMOUNT, GF_FINANCIAL_STATEMENTS_ID)
  lazy val PRIORITY ="priority"
  lazy val SOURCE ="source"
  lazy val V_ASSET_ID = "V_0101"
  lazy val PAR_GF_CUSTOMER_SALES_AMOUNT = "gf_customer_sales_amount"
  lazy val PAR_GF_TOTAL_ASSET_AMOUNT = "gf_total_asset_amount"
  lazy val PAR_GF_COMPANY_SIZE_DATE = "gf_company_size_date"
  lazy val PAR_GF_EMPLOYEES_NUMBER = "gf_employees_number"
  lazy val PAR_G_FFSS_STATUS_TYPE_3 = "g_ffss_status_type_3"
  lazy val PAR_CR_CUENTA_25 = "cr_cuenta_25"
  lazy val PARAMETER_NULL = "parameter_null"
  lazy val GF_CO_SIZE_CAL_TL_ASSET_AMOUNT= "gf_co_size_cal_tl_asset_amount"
  lazy val GF_CO_SIZE_CAL_EMPLYS_NUMBER= "gf_co_size_cal_emplys_number"
}
