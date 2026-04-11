# ------- ValuFlow - Staging Layer v4 ------- #
# ------- staging_v4.py ------- #

import snowflake.connector
from cryptography.hazmat.primitives import serialization
from dotenv import load_dotenv
from datetime import datetime
import os

load_dotenv(dotenv_path=r"C:\Users\timel\Desktop\ValuFlow\.env", override=True)

private_key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
with open(private_key_path, "rb") as key_file:
    private_key = serialization.load_pem_private_key(key_file.read(), password=None)

private_key_bytes = private_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption()
)

SNOWFLAKE_CONN = {
    "user": os.getenv("SNOWFLAKE_USER"),
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": os.getenv("SNOWFLAKE_DATABASE"),
    "private_key": private_key_bytes,
    "role": "SYSADMIN",
    "network_timeout": 300,
    "login_timeout": 60,
}

STAGING_TABLES = [

    ("INCOME_STMT_ANNUAL", "RAW.INCOME_STMT_ANNUAL", """
        CREATE OR REPLACE TABLE VALUFLOW.STAGING.INCOME_STMT_ANNUAL AS
        SELECT
            TICKER,
            TRY_TO_DATE(FMP_DATE)                                           AS PERIOD_DATE,
            REPORTEDCURRENCY,
            CIK,
            TRY_TO_DATE(FILINGDATE)                                         AS FILING_DATE,
            FISCALYEAR,
            PERIOD,
            TRY_TO_NUMBER(REVENUE, 38, 2)                                   AS REVENUE,
            TRY_TO_NUMBER(COSTOFREVENUE, 38, 2)                             AS COST_OF_REVENUE,
            TRY_TO_NUMBER(GROSSPROFIT, 38, 2)                               AS GROSS_PROFIT,
            TRY_TO_NUMBER(RESEARCHANDDEVELOPMENTEXPENSES, 38, 2)            AS RD_EXPENSES,
            TRY_TO_NUMBER(GENERALANDADMINISTRATIVEEXPENSES, 38, 2)          AS GA_EXPENSES,
            TRY_TO_NUMBER(SELLINGANDMARKETINGEXPENSES, 38, 2)               AS SELLING_EXPENSES,
            TRY_TO_NUMBER(SELLINGGENERALANDADMINISTRATIVEEXPENSES, 38, 2)   AS SGA_EXPENSES,
            TRY_TO_NUMBER(OTHEREXPENSES, 38, 2)                             AS OTHER_EXPENSES,
            TRY_TO_NUMBER(OPERATINGEXPENSES, 38, 2)                         AS OPERATING_EXPENSES,
            TRY_TO_NUMBER(COSTANDEXPENSES, 38, 2)                           AS COST_AND_EXPENSES,
            TRY_TO_NUMBER(NETINTERESTINCOME, 38, 2)                         AS NET_INTEREST_INCOME,
            TRY_TO_NUMBER(INTERESTINCOME, 38, 2)                            AS INTEREST_INCOME,
            TRY_TO_NUMBER(INTERESTEXPENSE, 38, 2)                           AS INTEREST_EXPENSE,
            TRY_TO_NUMBER(DEPRECIATIONANDAMORTIZATION, 38, 2)               AS DA,
            TRY_TO_NUMBER(EBITDA, 38, 2)                                    AS EBITDA,
            TRY_TO_NUMBER(EBIT, 38, 2)                                      AS EBIT,
            TRY_TO_NUMBER(NONOPERATINGINCOMEEXCLUDINGINTEREST, 38, 2)       AS NON_OPERATING_INCOME,
            TRY_TO_NUMBER(OPERATINGINCOME, 38, 2)                           AS OPERATING_INCOME,
            TRY_TO_NUMBER(TOTALOTHERINCOMEEXPENSESNET, 38, 2)               AS OTHER_INCOME_EXPENSE,
            TRY_TO_NUMBER(INCOMEBEFORETAX, 38, 2)                           AS INCOME_BEFORE_TAX,
            TRY_TO_NUMBER(INCOMETAXEXPENSE, 38, 2)                          AS INCOME_TAX_EXPENSE,
            TRY_TO_NUMBER(NETINCOMEFROMCONTINUINGOPERATIONS, 38, 2)         AS NET_INCOME_CONTINUING,
            TRY_TO_NUMBER(NETINCOMEFROMDISCONTINUEDOPERATIONS, 38, 2)       AS NET_INCOME_DISCONTINUED,
            TRY_TO_NUMBER(NETINCOME, 38, 2)                                 AS NET_INCOME,
            TRY_TO_NUMBER(BOTTOMLINENETINCOME, 38, 2)                       AS BOTTOM_LINE_NET_INCOME,
            TRY_TO_NUMBER(EPS, 38, 10)                                      AS EPS,
            TRY_TO_NUMBER(EPSDILUTED, 38, 10)                               AS EPS_DILUTED,
            TRY_TO_NUMBER(WEIGHTEDAVERAGESHSOUT, 38, 2)                     AS SHARES_OUTSTANDING,
            TRY_TO_NUMBER(WEIGHTEDAVERAGESHSOUTDIL, 38, 2)                  AS SHARES_OUTSTANDING_DIL
        FROM VALUFLOW.RAW.INCOME_STMT_ANNUAL
    """),

    ("INCOME_STMT_QUARTERLY", "RAW.INCOME_STMT_QUARTERLY", """
        CREATE OR REPLACE TABLE VALUFLOW.STAGING.INCOME_STMT_QUARTERLY AS
        SELECT
            TICKER,
            TRY_TO_DATE(FMP_DATE)                                           AS PERIOD_DATE,
            REPORTEDCURRENCY,
            CIK,
            TRY_TO_DATE(FILINGDATE)                                         AS FILING_DATE,
            FISCALYEAR,
            PERIOD,
            TRY_TO_NUMBER(REVENUE, 38, 2)                                   AS REVENUE,
            TRY_TO_NUMBER(COSTOFREVENUE, 38, 2)                             AS COST_OF_REVENUE,
            TRY_TO_NUMBER(GROSSPROFIT, 38, 2)                               AS GROSS_PROFIT,
            TRY_TO_NUMBER(RESEARCHANDDEVELOPMENTEXPENSES, 38, 2)            AS RD_EXPENSES,
            TRY_TO_NUMBER(GENERALANDADMINISTRATIVEEXPENSES, 38, 2)          AS GA_EXPENSES,
            TRY_TO_NUMBER(SELLINGANDMARKETINGEXPENSES, 38, 2)               AS SELLING_EXPENSES,
            TRY_TO_NUMBER(SELLINGGENERALANDADMINISTRATIVEEXPENSES, 38, 2)   AS SGA_EXPENSES,
            TRY_TO_NUMBER(OTHEREXPENSES, 38, 2)                             AS OTHER_EXPENSES,
            TRY_TO_NUMBER(OPERATINGEXPENSES, 38, 2)                         AS OPERATING_EXPENSES,
            TRY_TO_NUMBER(COSTANDEXPENSES, 38, 2)                           AS COST_AND_EXPENSES,
            TRY_TO_NUMBER(NETINTERESTINCOME, 38, 2)                         AS NET_INTEREST_INCOME,
            TRY_TO_NUMBER(INTERESTINCOME, 38, 2)                            AS INTEREST_INCOME,
            TRY_TO_NUMBER(INTERESTEXPENSE, 38, 2)                           AS INTEREST_EXPENSE,
            TRY_TO_NUMBER(DEPRECIATIONANDAMORTIZATION, 38, 2)               AS DA,
            TRY_TO_NUMBER(EBITDA, 38, 2)                                    AS EBITDA,
            TRY_TO_NUMBER(EBIT, 38, 2)                                      AS EBIT,
            TRY_TO_NUMBER(NONOPERATINGINCOMEEXCLUDINGINTEREST, 38, 2)       AS NON_OPERATING_INCOME,
            TRY_TO_NUMBER(OPERATINGINCOME, 38, 2)                           AS OPERATING_INCOME,
            TRY_TO_NUMBER(TOTALOTHERINCOMEEXPENSESNET, 38, 2)               AS OTHER_INCOME_EXPENSE,
            TRY_TO_NUMBER(INCOMEBEFORETAX, 38, 2)                           AS INCOME_BEFORE_TAX,
            TRY_TO_NUMBER(INCOMETAXEXPENSE, 38, 2)                          AS INCOME_TAX_EXPENSE,
            TRY_TO_NUMBER(NETINCOMEFROMCONTINUINGOPERATIONS, 38, 2)         AS NET_INCOME_CONTINUING,
            TRY_TO_NUMBER(NETINCOMEFROMDISCONTINUEDOPERATIONS, 38, 2)       AS NET_INCOME_DISCONTINUED,
            TRY_TO_NUMBER(NETINCOME, 38, 2)                                 AS NET_INCOME,
            TRY_TO_NUMBER(BOTTOMLINENETINCOME, 38, 2)                       AS BOTTOM_LINE_NET_INCOME,
            TRY_TO_NUMBER(EPS, 38, 10)                                      AS EPS,
            TRY_TO_NUMBER(EPSDILUTED, 38, 10)                               AS EPS_DILUTED,
            TRY_TO_NUMBER(WEIGHTEDAVERAGESHSOUT, 38, 2)                     AS SHARES_OUTSTANDING,
            TRY_TO_NUMBER(WEIGHTEDAVERAGESHSOUTDIL, 38, 2)                  AS SHARES_OUTSTANDING_DIL
        FROM VALUFLOW.RAW.INCOME_STMT_QUARTERLY
    """),

    ("BALANCE_SHEET_ANNUAL", "RAW.BALANCE_SHEET_ANNUAL", """
        CREATE OR REPLACE TABLE VALUFLOW.STAGING.BALANCE_SHEET_ANNUAL AS
        SELECT
            TICKER,
            TRY_TO_DATE(FMP_DATE)                                               AS PERIOD_DATE,
            REPORTEDCURRENCY,
            CIK,
            TRY_TO_DATE(FILINGDATE)                                             AS FILING_DATE,
            FISCALYEAR,
            PERIOD,
            TRY_TO_NUMBER(CASHANDCASHEQUIVALENTS, 38, 2)                        AS CASH_AND_EQUIVALENTS,
            TRY_TO_NUMBER(SHORTTERMINVESTMENTS, 38, 2)                          AS SHORT_TERM_INVESTMENTS,
            TRY_TO_NUMBER(CASHANDSHORTTERMINVESTMENTS, 38, 2)                   AS CASH_AND_ST_INVESTMENTS,
            TRY_TO_NUMBER(NETRECEIVABLES, 38, 2)                                AS NET_RECEIVABLES,
            TRY_TO_NUMBER(ACCOUNTSRECEIVABLES, 38, 2)                           AS ACCOUNTS_RECEIVABLE,
            TRY_TO_NUMBER(OTHERRECEIVABLES, 38, 2)                              AS OTHER_RECEIVABLES,
            TRY_TO_NUMBER(INVENTORY, 38, 2)                                     AS INVENTORY,
            TRY_TO_NUMBER(PREPAIDS, 38, 2)                                      AS PREPAIDS,
            TRY_TO_NUMBER(OTHERCURRENTASSETS, 38, 2)                            AS OTHER_CURRENT_ASSETS,
            TRY_TO_NUMBER(TOTALCURRENTASSETS, 38, 2)                            AS TOTAL_CURRENT_ASSETS,
            TRY_TO_NUMBER(PROPERTYPLANTEQUIPMENTNET, 38, 2)                     AS PP_AND_E_NET,
            TRY_TO_NUMBER(GOODWILL, 38, 2)                                      AS GOODWILL,
            TRY_TO_NUMBER(INTANGIBLEASSETS, 38, 2)                              AS INTANGIBLE_ASSETS,
            TRY_TO_NUMBER(GOODWILLANDINTANGIBLEASSETS, 38, 2)                   AS GOODWILL_AND_INTANGIBLES,
            TRY_TO_NUMBER(LONGTERMINVESTMENTS, 38, 2)                           AS LONG_TERM_INVESTMENTS,
            TRY_TO_NUMBER(TAXASSETS, 38, 2)                                     AS TAX_ASSETS,
            TRY_TO_NUMBER(OTHERNONCURRENTASSETS, 38, 2)                         AS OTHER_NON_CURRENT_ASSETS,
            TRY_TO_NUMBER(TOTALNONCURRENTASSETS, 38, 2)                         AS TOTAL_NON_CURRENT_ASSETS,
            TRY_TO_NUMBER(OTHERASSETS, 38, 2)                                   AS OTHER_ASSETS,
            TRY_TO_NUMBER(TOTALASSETS, 38, 2)                                   AS TOTAL_ASSETS,
            TRY_TO_NUMBER(TOTALPAYABLES, 38, 2)                                 AS TOTAL_PAYABLES,
            TRY_TO_NUMBER(ACCOUNTPAYABLES, 38, 2)                               AS ACCOUNTS_PAYABLE,
            TRY_TO_NUMBER(OTHERPAYABLES, 38, 2)                                 AS OTHER_PAYABLES,
            TRY_TO_NUMBER(ACCRUEDEXPENSES, 38, 2)                               AS ACCRUED_EXPENSES,
            TRY_TO_NUMBER(SHORTTERMDEBT, 38, 2)                                 AS SHORT_TERM_DEBT,
            TRY_TO_NUMBER(CAPITALLEASEOBLIGATIONSCURRENT, 38, 2)                AS CAPITAL_LEASE_CURRENT,
            TRY_TO_NUMBER(TAXPAYABLES, 38, 2)                                   AS TAX_PAYABLES,
            TRY_TO_NUMBER(DEFERREDREVENUE, 38, 2)                               AS DEFERRED_REVENUE,
            TRY_TO_NUMBER(OTHERCURRENTLIABILITIES, 38, 2)                       AS OTHER_CURRENT_LIABILITIES,
            TRY_TO_NUMBER(TOTALCURRENTLIABILITIES, 38, 2)                       AS TOTAL_CURRENT_LIABILITIES,
            TRY_TO_NUMBER(LONGTERMDEBT, 38, 2)                                  AS LONG_TERM_DEBT,
            TRY_TO_NUMBER(CAPITALLEASEOBLIGATIONSNONCURRENT, 38, 2)             AS CAPITAL_LEASE_NON_CURRENT,
            TRY_TO_NUMBER(DEFERREDREVENUENONCURRENT, 38, 2)                     AS DEFERRED_REVENUE_NON_CURRENT,
            TRY_TO_NUMBER(DEFERREDTAXLIABILITIESNONCURRENT, 38, 2)              AS DEFERRED_TAX_LIABILITIES,
            TRY_TO_NUMBER(OTHERNONCURRENTLIABILITIES, 38, 2)                    AS OTHER_NON_CURRENT_LIABILITIES,
            TRY_TO_NUMBER(TOTALNONCURRENTLIABILITIES, 38, 2)                    AS TOTAL_NON_CURRENT_LIABILITIES,
            TRY_TO_NUMBER(OTHERLIABILITIES, 38, 2)                              AS OTHER_LIABILITIES,
            TRY_TO_NUMBER(CAPITALLEASEOBLIGATIONS, 38, 2)                       AS CAPITAL_LEASE_OBLIGATIONS,
            TRY_TO_NUMBER(TOTALLIABILITIES, 38, 2)                              AS TOTAL_LIABILITIES,
            TRY_TO_NUMBER(TREASURYSTOCK, 38, 2)                                 AS TREASURY_STOCK,
            TRY_TO_NUMBER(PREFERREDSTOCK, 38, 2)                                AS PREFERRED_STOCK,
            TRY_TO_NUMBER(COMMONSTOCK, 38, 2)                                   AS COMMON_STOCK,
            TRY_TO_NUMBER(RETAINEDEARNINGS, 38, 2)                              AS RETAINED_EARNINGS,
            TRY_TO_NUMBER(ADDITIONALPAIDINCAPITAL, 38, 2)                       AS ADDITIONAL_PAID_IN_CAPITAL,
            TRY_TO_NUMBER(ACCUMULATEDOTHERCOMPREHENSIVEINCOMELOSS, 38, 2)       AS AOCI,
            TRY_TO_NUMBER(OTHERTOTALSTOCKHOLDERSEQUITY, 38, 2)                  AS OTHER_STOCKHOLDERS_EQUITY,
            TRY_TO_NUMBER(TOTALSTOCKHOLDERSEQUITY, 38, 2)                       AS TOTAL_STOCKHOLDERS_EQUITY,
            TRY_TO_NUMBER(TOTALEQUITY, 38, 2)                                   AS TOTAL_EQUITY,
            TRY_TO_NUMBER(MINORITYINTEREST, 38, 2)                              AS MINORITY_INTEREST,
            TRY_TO_NUMBER(TOTALLIABILITIESANDTOTALEQUITY, 38, 2)                AS TOTAL_LIABILITIES_AND_EQUITY,
            TRY_TO_NUMBER(TOTALINVESTMENTS, 38, 2)                              AS TOTAL_INVESTMENTS,
            TRY_TO_NUMBER(TOTALDEBT, 38, 2)                                     AS TOTAL_DEBT,
            TRY_TO_NUMBER(NETDEBT, 38, 2)                                       AS NET_DEBT
        FROM VALUFLOW.RAW.BALANCE_SHEET_ANNUAL
    """),

    ("BALANCE_SHEET_QUARTERLY", "RAW.BALANCE_SHEET_QUARTERLY", """
        CREATE OR REPLACE TABLE VALUFLOW.STAGING.BALANCE_SHEET_QUARTERLY AS
        SELECT
            TICKER,
            TRY_TO_DATE(FMP_DATE)                                               AS PERIOD_DATE,
            REPORTEDCURRENCY,
            CIK,
            TRY_TO_DATE(FILINGDATE)                                             AS FILING_DATE,
            FISCALYEAR,
            PERIOD,
            TRY_TO_NUMBER(CASHANDCASHEQUIVALENTS, 38, 2)                        AS CASH_AND_EQUIVALENTS,
            TRY_TO_NUMBER(SHORTTERMINVESTMENTS, 38, 2)                          AS SHORT_TERM_INVESTMENTS,
            TRY_TO_NUMBER(CASHANDSHORTTERMINVESTMENTS, 38, 2)                   AS CASH_AND_ST_INVESTMENTS,
            TRY_TO_NUMBER(NETRECEIVABLES, 38, 2)                                AS NET_RECEIVABLES,
            TRY_TO_NUMBER(ACCOUNTSRECEIVABLES, 38, 2)                           AS ACCOUNTS_RECEIVABLE,
            TRY_TO_NUMBER(OTHERRECEIVABLES, 38, 2)                              AS OTHER_RECEIVABLES,
            TRY_TO_NUMBER(INVENTORY, 38, 2)                                     AS INVENTORY,
            TRY_TO_NUMBER(PREPAIDS, 38, 2)                                      AS PREPAIDS,
            TRY_TO_NUMBER(OTHERCURRENTASSETS, 38, 2)                            AS OTHER_CURRENT_ASSETS,
            TRY_TO_NUMBER(TOTALCURRENTASSETS, 38, 2)                            AS TOTAL_CURRENT_ASSETS,
            TRY_TO_NUMBER(PROPERTYPLANTEQUIPMENTNET, 38, 2)                     AS PP_AND_E_NET,
            TRY_TO_NUMBER(GOODWILL, 38, 2)                                      AS GOODWILL,
            TRY_TO_NUMBER(INTANGIBLEASSETS, 38, 2)                              AS INTANGIBLE_ASSETS,
            TRY_TO_NUMBER(GOODWILLANDINTANGIBLEASSETS, 38, 2)                   AS GOODWILL_AND_INTANGIBLES,
            TRY_TO_NUMBER(LONGTERMINVESTMENTS, 38, 2)                           AS LONG_TERM_INVESTMENTS,
            TRY_TO_NUMBER(TAXASSETS, 38, 2)                                     AS TAX_ASSETS,
            TRY_TO_NUMBER(OTHERNONCURRENTASSETS, 38, 2)                         AS OTHER_NON_CURRENT_ASSETS,
            TRY_TO_NUMBER(TOTALNONCURRENTASSETS, 38, 2)                         AS TOTAL_NON_CURRENT_ASSETS,
            TRY_TO_NUMBER(OTHERASSETS, 38, 2)                                   AS OTHER_ASSETS,
            TRY_TO_NUMBER(TOTALASSETS, 38, 2)                                   AS TOTAL_ASSETS,
            TRY_TO_NUMBER(TOTALPAYABLES, 38, 2)                                 AS TOTAL_PAYABLES,
            TRY_TO_NUMBER(ACCOUNTPAYABLES, 38, 2)                               AS ACCOUNTS_PAYABLE,
            TRY_TO_NUMBER(OTHERPAYABLES, 38, 2)                                 AS OTHER_PAYABLES,
            TRY_TO_NUMBER(ACCRUEDEXPENSES, 38, 2)                               AS ACCRUED_EXPENSES,
            TRY_TO_NUMBER(SHORTTERMDEBT, 38, 2)                                 AS SHORT_TERM_DEBT,
            TRY_TO_NUMBER(CAPITALLEASEOBLIGATIONSCURRENT, 38, 2)                AS CAPITAL_LEASE_CURRENT,
            TRY_TO_NUMBER(TAXPAYABLES, 38, 2)                                   AS TAX_PAYABLES,
            TRY_TO_NUMBER(DEFERREDREVENUE, 38, 2)                               AS DEFERRED_REVENUE,
            TRY_TO_NUMBER(OTHERCURRENTLIABILITIES, 38, 2)                       AS OTHER_CURRENT_LIABILITIES,
            TRY_TO_NUMBER(TOTALCURRENTLIABILITIES, 38, 2)                       AS TOTAL_CURRENT_LIABILITIES,
            TRY_TO_NUMBER(LONGTERMDEBT, 38, 2)                                  AS LONG_TERM_DEBT,
            TRY_TO_NUMBER(CAPITALLEASEOBLIGATIONSNONCURRENT, 38, 2)             AS CAPITAL_LEASE_NON_CURRENT,
            TRY_TO_NUMBER(DEFERREDREVENUENONCURRENT, 38, 2)                     AS DEFERRED_REVENUE_NON_CURRENT,
            TRY_TO_NUMBER(DEFERREDTAXLIABILITIESNONCURRENT, 38, 2)              AS DEFERRED_TAX_LIABILITIES,
            TRY_TO_NUMBER(OTHERNONCURRENTLIABILITIES, 38, 2)                    AS OTHER_NON_CURRENT_LIABILITIES,
            TRY_TO_NUMBER(TOTALNONCURRENTLIABILITIES, 38, 2)                    AS TOTAL_NON_CURRENT_LIABILITIES,
            TRY_TO_NUMBER(OTHERLIABILITIES, 38, 2)                              AS OTHER_LIABILITIES,
            TRY_TO_NUMBER(CAPITALLEASEOBLIGATIONS, 38, 2)                       AS CAPITAL_LEASE_OBLIGATIONS,
            TRY_TO_NUMBER(TOTALLIABILITIES, 38, 2)                              AS TOTAL_LIABILITIES,
            TRY_TO_NUMBER(TREASURYSTOCK, 38, 2)                                 AS TREASURY_STOCK,
            TRY_TO_NUMBER(PREFERREDSTOCK, 38, 2)                                AS PREFERRED_STOCK,
            TRY_TO_NUMBER(COMMONSTOCK, 38, 2)                                   AS COMMON_STOCK,
            TRY_TO_NUMBER(RETAINEDEARNINGS, 38, 2)                              AS RETAINED_EARNINGS,
            TRY_TO_NUMBER(ADDITIONALPAIDINCAPITAL, 38, 2)                       AS ADDITIONAL_PAID_IN_CAPITAL,
            TRY_TO_NUMBER(ACCUMULATEDOTHERCOMPREHENSIVEINCOMELOSS, 38, 2)       AS AOCI,
            TRY_TO_NUMBER(OTHERTOTALSTOCKHOLDERSEQUITY, 38, 2)                  AS OTHER_STOCKHOLDERS_EQUITY,
            TRY_TO_NUMBER(TOTALSTOCKHOLDERSEQUITY, 38, 2)                       AS TOTAL_STOCKHOLDERS_EQUITY,
            TRY_TO_NUMBER(TOTALEQUITY, 38, 2)                                   AS TOTAL_EQUITY,
            TRY_TO_NUMBER(MINORITYINTEREST, 38, 2)                              AS MINORITY_INTEREST,
            TRY_TO_NUMBER(TOTALLIABILITIESANDTOTALEQUITY, 38, 2)                AS TOTAL_LIABILITIES_AND_EQUITY,
            TRY_TO_NUMBER(TOTALINVESTMENTS, 38, 2)                              AS TOTAL_INVESTMENTS,
            TRY_TO_NUMBER(TOTALDEBT, 38, 2)                                     AS TOTAL_DEBT,
            TRY_TO_NUMBER(NETDEBT, 38, 2)                                       AS NET_DEBT
        FROM VALUFLOW.RAW.BALANCE_SHEET_QUARTERLY
    """),

    ("CASH_FLOW_ANNUAL", "RAW.CASH_FLOW_ANNUAL", """
        CREATE OR REPLACE TABLE VALUFLOW.STAGING.CASH_FLOW_ANNUAL AS
        SELECT
            TICKER,
            TRY_TO_DATE(FMP_DATE)                                               AS PERIOD_DATE,
            REPORTEDCURRENCY,
            CIK,
            TRY_TO_DATE(FILINGDATE)                                             AS FILING_DATE,
            FISCALYEAR,
            PERIOD,
            TRY_TO_NUMBER(NETINCOME, 38, 2)                                     AS NET_INCOME,
            TRY_TO_NUMBER(DEPRECIATIONANDAMORTIZATION, 38, 2)                   AS DA,
            TRY_TO_NUMBER(DEFERREDINCOMETAX, 38, 2)                             AS DEFERRED_INCOME_TAX,
            TRY_TO_NUMBER(STOCKBASEDCOMPENSATION, 38, 2)                        AS STOCK_BASED_COMPENSATION,
            TRY_TO_NUMBER(CHANGEINWORKINGCAPITAL, 38, 2)                        AS CHANGE_IN_WORKING_CAPITAL,
            TRY_TO_NUMBER(ACCOUNTSRECEIVABLES, 38, 2)                           AS ACCOUNTS_RECEIVABLE,
            TRY_TO_NUMBER(INVENTORY, 38, 2)                                     AS INVENTORY,
            TRY_TO_NUMBER(ACCOUNTSPAYABLES, 38, 2)                              AS ACCOUNTS_PAYABLE,
            TRY_TO_NUMBER(OTHERWORKINGCAPITAL, 38, 2)                           AS OTHER_WORKING_CAPITAL,
            TRY_TO_NUMBER(OTHERNONCASHITEMS, 38, 2)                             AS OTHER_NON_CASH_ITEMS,
            TRY_TO_NUMBER(NETCASHPROVIDEDBYOPERATINGACTIVITIES, 38, 2)          AS OPERATING_CASH_FLOW,
            TRY_TO_NUMBER(INVESTMENTSINPROPERTYPLANTANDEQUIPMENT, 38, 2)        AS CAPEX,
            TRY_TO_NUMBER(ACQUISITIONSNET, 38, 2)                               AS ACQUISITIONS_NET,
            TRY_TO_NUMBER(PURCHASESOFINVESTMENTS, 38, 2)                        AS PURCHASES_OF_INVESTMENTS,
            TRY_TO_NUMBER(SALESMATURITIESOFINVESTMENTS, 38, 2)                  AS SALES_OF_INVESTMENTS,
            TRY_TO_NUMBER(OTHERINVESTINGACTIVITIES, 38, 2)                      AS OTHER_INVESTING_ACTIVITIES,
            TRY_TO_NUMBER(NETCASHPROVIDEDBYINVESTINGACTIVITIES, 38, 2)          AS INVESTING_CASH_FLOW,
            TRY_TO_NUMBER(NETDEBTISSUANCE, 38, 2)                               AS NET_DEBT_ISSUANCE,
            TRY_TO_NUMBER(LONGTERMNETDEBTISSUANCE, 38, 2)                       AS LT_DEBT_ISSUANCE,
            TRY_TO_NUMBER(SHORTTERMNETDEBTISSUANCE, 38, 2)                      AS ST_DEBT_ISSUANCE,
            TRY_TO_NUMBER(NETSTOCKISSUANCE, 38, 2)                              AS NET_STOCK_ISSUANCE,
            TRY_TO_NUMBER(NETCOMMONSTOCKISSUANCE, 38, 2)                        AS COMMON_STOCK_ISSUANCE,
            TRY_TO_NUMBER(COMMONSTOCKREPURCHASED, 38, 2)                        AS COMMON_STOCK_REPURCHASED,
            TRY_TO_NUMBER(NETDIVIDENDSPAID, 38, 2)                              AS NET_DIVIDENDS_PAID,
            TRY_TO_NUMBER(COMMONDIVIDENDSPAID, 38, 2)                           AS COMMON_DIVIDENDS_PAID,
            TRY_TO_NUMBER(OTHERFINANCINGACTIVITIES, 38, 2)                      AS OTHER_FINANCING_ACTIVITIES,
            TRY_TO_NUMBER(NETCASHPROVIDEDBYFINANCINGACTIVITIES, 38, 2)          AS FINANCING_CASH_FLOW,
            TRY_TO_NUMBER(EFFECTOFFOREXCHANGESONCASH, 38, 2)                    AS FOREX_EFFECT_ON_CASH,
            TRY_TO_NUMBER(NETCHANGEINCASH, 38, 2)                               AS NET_CHANGE_IN_CASH,
            TRY_TO_NUMBER(CASHATENDOFPERIOD, 38, 2)                             AS CASH_END_OF_PERIOD,
            TRY_TO_NUMBER(CASHATBEGINNINGOFPERIOD, 38, 2)                       AS CASH_BEGIN_OF_PERIOD,
            TRY_TO_NUMBER(OPERATINGCASHFLOW, 38, 2)                             AS OPERATING_CASH_FLOW_ALT,
            TRY_TO_NUMBER(CAPITALEXPENDITURE, 38, 2)                            AS CAPITAL_EXPENDITURE,
            TRY_TO_NUMBER(FREECASHFLOW, 38, 2)                                  AS FREE_CASH_FLOW,
            TRY_TO_NUMBER(INCOMETAXESPAID, 38, 2)                               AS INCOME_TAXES_PAID,
            TRY_TO_NUMBER(INTERESTPAID, 38, 2)                                  AS INTEREST_PAID
        FROM VALUFLOW.RAW.CASH_FLOW_ANNUAL
    """),

    ("CASH_FLOW_QUARTERLY", "RAW.CASH_FLOW_QUARTERLY", """
        CREATE OR REPLACE TABLE VALUFLOW.STAGING.CASH_FLOW_QUARTERLY AS
        SELECT
            TICKER,
            TRY_TO_DATE(FMP_DATE)                                               AS PERIOD_DATE,
            REPORTEDCURRENCY,
            CIK,
            TRY_TO_DATE(FILINGDATE)                                             AS FILING_DATE,
            FISCALYEAR,
            PERIOD,
            TRY_TO_NUMBER(NETINCOME, 38, 2)                                     AS NET_INCOME,
            TRY_TO_NUMBER(DEPRECIATIONANDAMORTIZATION, 38, 2)                   AS DA,
            TRY_TO_NUMBER(DEFERREDINCOMETAX, 38, 2)                             AS DEFERRED_INCOME_TAX,
            TRY_TO_NUMBER(STOCKBASEDCOMPENSATION, 38, 2)                        AS STOCK_BASED_COMPENSATION,
            TRY_TO_NUMBER(CHANGEINWORKINGCAPITAL, 38, 2)                        AS CHANGE_IN_WORKING_CAPITAL,
            TRY_TO_NUMBER(ACCOUNTSRECEIVABLES, 38, 2)                           AS ACCOUNTS_RECEIVABLE,
            TRY_TO_NUMBER(INVENTORY, 38, 2)                                     AS INVENTORY,
            TRY_TO_NUMBER(ACCOUNTSPAYABLES, 38, 2)                              AS ACCOUNTS_PAYABLE,
            TRY_TO_NUMBER(OTHERWORKINGCAPITAL, 38, 2)                           AS OTHER_WORKING_CAPITAL,
            TRY_TO_NUMBER(OTHERNONCASHITEMS, 38, 2)                             AS OTHER_NON_CASH_ITEMS,
            TRY_TO_NUMBER(NETCASHPROVIDEDBYOPERATINGACTIVITIES, 38, 2)          AS OPERATING_CASH_FLOW,
            TRY_TO_NUMBER(INVESTMENTSINPROPERTYPLANTANDEQUIPMENT, 38, 2)        AS CAPEX,
            TRY_TO_NUMBER(ACQUISITIONSNET, 38, 2)                               AS ACQUISITIONS_NET,
            TRY_TO_NUMBER(PURCHASESOFINVESTMENTS, 38, 2)                        AS PURCHASES_OF_INVESTMENTS,
            TRY_TO_NUMBER(SALESMATURITIESOFINVESTMENTS, 38, 2)                  AS SALES_OF_INVESTMENTS,
            TRY_TO_NUMBER(OTHERINVESTINGACTIVITIES, 38, 2)                      AS OTHER_INVESTING_ACTIVITIES,
            TRY_TO_NUMBER(NETCASHPROVIDEDBYINVESTINGACTIVITIES, 38, 2)          AS INVESTING_CASH_FLOW,
            TRY_TO_NUMBER(NETDEBTISSUANCE, 38, 2)                               AS NET_DEBT_ISSUANCE,
            TRY_TO_NUMBER(LONGTERMNETDEBTISSUANCE, 38, 2)                       AS LT_DEBT_ISSUANCE,
            TRY_TO_NUMBER(SHORTTERMNETDEBTISSUANCE, 38, 2)                      AS ST_DEBT_ISSUANCE,
            TRY_TO_NUMBER(NETSTOCKISSUANCE, 38, 2)                              AS NET_STOCK_ISSUANCE,
            TRY_TO_NUMBER(NETCOMMONSTOCKISSUANCE, 38, 2)                        AS COMMON_STOCK_ISSUANCE,
            TRY_TO_NUMBER(COMMONSTOCKREPURCHASED, 38, 2)                        AS COMMON_STOCK_REPURCHASED,
            TRY_TO_NUMBER(NETDIVIDENDSPAID, 38, 2)                              AS NET_DIVIDENDS_PAID,
            TRY_TO_NUMBER(COMMONDIVIDENDSPAID, 38, 2)                           AS COMMON_DIVIDENDS_PAID,
            TRY_TO_NUMBER(OTHERFINANCINGACTIVITIES, 38, 2)                      AS OTHER_FINANCING_ACTIVITIES,
            TRY_TO_NUMBER(NETCASHPROVIDEDBYFINANCINGACTIVITIES, 38, 2)          AS FINANCING_CASH_FLOW,
            TRY_TO_NUMBER(EFFECTOFFOREXCHANGESONCASH, 38, 2)                    AS FOREX_EFFECT_ON_CASH,
            TRY_TO_NUMBER(NETCHANGEINCASH, 38, 2)                               AS NET_CHANGE_IN_CASH,
            TRY_TO_NUMBER(CASHATENDOFPERIOD, 38, 2)                             AS CASH_END_OF_PERIOD,
            TRY_TO_NUMBER(CASHATBEGINNINGOFPERIOD, 38, 2)                       AS CASH_BEGIN_OF_PERIOD,
            TRY_TO_NUMBER(OPERATINGCASHFLOW, 38, 2)                             AS OPERATING_CASH_FLOW_ALT,
            TRY_TO_NUMBER(CAPITALEXPENDITURE, 38, 2)                            AS CAPITAL_EXPENDITURE,
            TRY_TO_NUMBER(FREECASHFLOW, 38, 2)                                  AS FREE_CASH_FLOW,
            TRY_TO_NUMBER(INCOMETAXESPAID, 38, 2)                               AS INCOME_TAXES_PAID,
            TRY_TO_NUMBER(INTERESTPAID, 38, 2)                                  AS INTEREST_PAID
        FROM VALUFLOW.RAW.CASH_FLOW_QUARTERLY
    """),

    ("KEY_METRICS_ANNUAL", "RAW.KEY_METRICS_ANNUAL", """
        CREATE OR REPLACE TABLE VALUFLOW.STAGING.KEY_METRICS_ANNUAL AS
        SELECT
            TICKER,
            TRY_TO_DATE(FMP_DATE)                                               AS PERIOD_DATE,
            FISCALYEAR,
            PERIOD,
            REPORTEDCURRENCY,
            TRY_TO_NUMBER(MARKETCAP, 38, 2)                                     AS MARKET_CAP,
            TRY_TO_NUMBER(ENTERPRISEVALUE, 38, 2)                               AS ENTERPRISE_VALUE,
            TRY_TO_NUMBER(EVTOSALES, 38, 6)                                     AS EV_TO_SALES,
            TRY_TO_NUMBER(EVTOOPERATINGCASHFLOW, 38, 6)                         AS EV_TO_OCF,
            TRY_TO_NUMBER(EVTOFREECASHFLOW, 38, 6)                              AS EV_TO_FCF,
            TRY_TO_NUMBER(EVTOEBITDA, 38, 6)                                    AS EV_TO_EBITDA,
            TRY_TO_NUMBER(NETDEBTTOEBITDA, 38, 6)                               AS NET_DEBT_TO_EBITDA,
            TRY_TO_NUMBER(CURRENTRATIO, 38, 6)                                  AS CURRENT_RATIO,
            TRY_TO_NUMBER(GRAHAMNUMBER, 38, 6)                                  AS GRAHAM_NUMBER,
            TRY_TO_NUMBER(GRAHAMNETNET, 38, 6)                                  AS GRAHAM_NET_NET,
            TRY_TO_NUMBER(TAXBURDEN, 38, 6)                                     AS TAX_BURDEN,
            TRY_TO_NUMBER(INTERESTBURDEN, 38, 6)                                AS INTEREST_BURDEN,
            TRY_TO_NUMBER(WORKINGCAPITAL, 38, 2)                                AS WORKING_CAPITAL,
            TRY_TO_NUMBER(INVESTEDCAPITAL, 38, 2)                               AS INVESTED_CAPITAL,
            TRY_TO_NUMBER(RETURNONASSETS, 38, 6)                                AS RETURN_ON_ASSETS,
            TRY_TO_NUMBER(OPERATINGRETURNONASSETS, 38, 6)                       AS OPERATING_ROA,
            TRY_TO_NUMBER(RETURNONTANGIBLEASSETS, 38, 6)                        AS RETURN_ON_TANGIBLE_ASSETS,
            TRY_TO_NUMBER(RETURNONEQUITY, 38, 6)                                AS RETURN_ON_EQUITY,
            TRY_TO_NUMBER(RETURNONINVESTEDCAPITAL, 38, 6)                       AS ROIC,
            TRY_TO_NUMBER(RETURNONCAPITALEMPLOYED, 38, 6)                       AS ROCE,
            TRY_TO_NUMBER(EARNINGSYIELD, 38, 6)                                 AS EARNINGS_YIELD,
            TRY_TO_NUMBER(FREECASHFLOWYIELD, 38, 6)                             AS FCF_YIELD,
            TRY_TO_NUMBER(CAPEXTOOPERATINGCASHFLOW, 38, 6)                      AS CAPEX_TO_OCF,
            TRY_TO_NUMBER(CAPEXTODEPRECIATION, 38, 6)                           AS CAPEX_TO_DA,
            TRY_TO_NUMBER(CAPEXTOREVENUE, 38, 6)                                AS CAPEX_TO_REVENUE,
            TRY_TO_NUMBER(SALESGENERALANDADMINISTRATIVETOREVENUE, 38, 6)        AS SGA_TO_REVENUE,
            TRY_TO_NUMBER(RESEARCHANDDEVELOPEMENTTOREVENUE, 38, 6)              AS RD_TO_REVENUE,
            TRY_TO_NUMBER(STOCKBASEDCOMPENSATIONTOREVENUE, 38, 6)               AS SBC_TO_REVENUE,
            TRY_TO_NUMBER(INTANGIBLESTOTOTALASSETS, 38, 6)                     AS INTANGIBLES_TO_ASSETS,
            TRY_TO_NUMBER(AVERAGERECEIVABLES, 38, 2)                            AS AVG_RECEIVABLES,
            TRY_TO_NUMBER(AVERAGEPAYABLES, 38, 2)                               AS AVG_PAYABLES,
            TRY_TO_NUMBER(AVERAGEINVENTORY, 38, 2)                              AS AVG_INVENTORY,
            TRY_TO_NUMBER(DAYSOFSALESOUTSTANDING, 38, 2)                        AS DSO,
            TRY_TO_NUMBER(DAYSOFPAYABLESOUTSTANDING, 38, 2)                     AS DPO,
            TRY_TO_NUMBER(DAYSOFINVENTORYOUTSTANDING, 38, 2)                    AS DIO,
            TRY_TO_NUMBER(OPERATINGCYCLE, 38, 2)                                AS OPERATING_CYCLE,
            TRY_TO_NUMBER(CASHCONVERSIONCYCLE, 38, 2)                           AS CASH_CONVERSION_CYCLE,
            TRY_TO_NUMBER(FREECASHFLOWTOEQUITY, 38, 2)                          AS FCFE,
            TRY_TO_NUMBER(FREECASHFLOWTOFIRM, 38, 2)                            AS FCFF,
            TRY_TO_NUMBER(TANGIBLEASSETVALUE, 38, 2)                            AS TANGIBLE_ASSET_VALUE,
            TRY_TO_NUMBER(NETCURRENTASSETVALUE, 38, 2)                          AS NET_CURRENT_ASSET_VALUE
        FROM VALUFLOW.RAW.KEY_METRICS_ANNUAL
    """),

    ("KEY_METRICS_QUARTERLY", "RAW.KEY_METRICS_QUARTERLY", """
        CREATE OR REPLACE TABLE VALUFLOW.STAGING.KEY_METRICS_QUARTERLY AS
        SELECT
            TICKER,
            TRY_TO_DATE(FMP_DATE)                                               AS PERIOD_DATE,
            FISCALYEAR,
            PERIOD,
            REPORTEDCURRENCY,
            TRY_TO_NUMBER(MARKETCAP, 38, 2)                                     AS MARKET_CAP,
            TRY_TO_NUMBER(ENTERPRISEVALUE, 38, 2)                               AS ENTERPRISE_VALUE,
            TRY_TO_NUMBER(EVTOSALES, 38, 6)                                     AS EV_TO_SALES,
            TRY_TO_NUMBER(EVTOOPERATINGCASHFLOW, 38, 6)                         AS EV_TO_OCF,
            TRY_TO_NUMBER(EVTOFREECASHFLOW, 38, 6)                              AS EV_TO_FCF,
            TRY_TO_NUMBER(EVTOEBITDA, 38, 6)                                    AS EV_TO_EBITDA,
            TRY_TO_NUMBER(NETDEBTTOEBITDA, 38, 6)                               AS NET_DEBT_TO_EBITDA,
            TRY_TO_NUMBER(CURRENTRATIO, 38, 6)                                  AS CURRENT_RATIO,
            TRY_TO_NUMBER(GRAHAMNUMBER, 38, 6)                                  AS GRAHAM_NUMBER,
            TRY_TO_NUMBER(GRAHAMNETNET, 38, 6)                                  AS GRAHAM_NET_NET,
            TRY_TO_NUMBER(TAXBURDEN, 38, 6)                                     AS TAX_BURDEN,
            TRY_TO_NUMBER(INTERESTBURDEN, 38, 6)                                AS INTEREST_BURDEN,
            TRY_TO_NUMBER(WORKINGCAPITAL, 38, 2)                                AS WORKING_CAPITAL,
            TRY_TO_NUMBER(INVESTEDCAPITAL, 38, 2)                               AS INVESTED_CAPITAL,
            TRY_TO_NUMBER(RETURNONASSETS, 38, 6)                                AS RETURN_ON_ASSETS,
            TRY_TO_NUMBER(OPERATINGRETURNONASSETS, 38, 6)                       AS OPERATING_ROA,
            TRY_TO_NUMBER(RETURNONTANGIBLEASSETS, 38, 6)                        AS RETURN_ON_TANGIBLE_ASSETS,
            TRY_TO_NUMBER(RETURNONEQUITY, 38, 6)                                AS RETURN_ON_EQUITY,
            TRY_TO_NUMBER(RETURNONINVESTEDCAPITAL, 38, 6)                       AS ROIC,
            TRY_TO_NUMBER(RETURNONCAPITALEMPLOYED, 38, 6)                       AS ROCE,
            TRY_TO_NUMBER(EARNINGSYIELD, 38, 6)                                 AS EARNINGS_YIELD,
            TRY_TO_NUMBER(FREECASHFLOWYIELD, 38, 6)                             AS FCF_YIELD,
            TRY_TO_NUMBER(CAPEXTOOPERATINGCASHFLOW, 38, 6)                      AS CAPEX_TO_OCF,
            TRY_TO_NUMBER(CAPEXTODEPRECIATION, 38, 6)                           AS CAPEX_TO_DA,
            TRY_TO_NUMBER(CAPEXTOREVENUE, 38, 6)                                AS CAPEX_TO_REVENUE,
            TRY_TO_NUMBER(SALESGENERALANDADMINISTRATIVETOREVENUE, 38, 6)        AS SGA_TO_REVENUE,
            TRY_TO_NUMBER(RESEARCHANDDEVELOPEMENTTOREVENUE, 38, 6)              AS RD_TO_REVENUE,
            TRY_TO_NUMBER(STOCKBASEDCOMPENSATIONTOREVENUE, 38, 6)               AS SBC_TO_REVENUE,
            TRY_TO_NUMBER(INTANGIBLESTOTOTALASSETS, 38, 6)                     AS INTANGIBLES_TO_ASSETS,
            TRY_TO_NUMBER(AVERAGERECEIVABLES, 38, 2)                            AS AVG_RECEIVABLES,
            TRY_TO_NUMBER(AVERAGEPAYABLES, 38, 2)                               AS AVG_PAYABLES,
            TRY_TO_NUMBER(AVERAGEINVENTORY, 38, 2)                              AS AVG_INVENTORY,
            TRY_TO_NUMBER(DAYSOFSALESOUTSTANDING, 38, 2)                        AS DSO,
            TRY_TO_NUMBER(DAYSOFPAYABLESOUTSTANDING, 38, 2)                     AS DPO,
            TRY_TO_NUMBER(DAYSOFINVENTORYOUTSTANDING, 38, 2)                    AS DIO,
            TRY_TO_NUMBER(OPERATINGCYCLE, 38, 2)                                AS OPERATING_CYCLE,
            TRY_TO_NUMBER(CASHCONVERSIONCYCLE, 38, 2)                           AS CASH_CONVERSION_CYCLE,
            TRY_TO_NUMBER(FREECASHFLOWTOEQUITY, 38, 2)                          AS FCFE,
            TRY_TO_NUMBER(FREECASHFLOWTOFIRM, 38, 2)                            AS FCFF,
            TRY_TO_NUMBER(TANGIBLEASSETVALUE, 38, 2)                            AS TANGIBLE_ASSET_VALUE,
            TRY_TO_NUMBER(NETCURRENTASSETVALUE, 38, 2)                          AS NET_CURRENT_ASSET_VALUE
        FROM VALUFLOW.RAW.KEY_METRICS_QUARTERLY
    """),

    ("RATIOS_ANNUAL", "RAW.RATIOS_ANNUAL", """
        CREATE OR REPLACE TABLE VALUFLOW.STAGING.RATIOS_ANNUAL AS
        SELECT
            TICKER,
            TRY_TO_DATE(FMP_DATE)                                               AS PERIOD_DATE,
            FISCALYEAR,
            PERIOD,
            REPORTEDCURRENCY,
            TRY_TO_NUMBER(GROSSPROFITMARGIN, 38, 6)                             AS GROSS_PROFIT_MARGIN,
            TRY_TO_NUMBER(EBITMARGIN, 38, 6)                                    AS EBIT_MARGIN,
            TRY_TO_NUMBER(EBITDAMARGIN, 38, 6)                                  AS EBITDA_MARGIN,
            TRY_TO_NUMBER(OPERATINGPROFITMARGIN, 38, 6)                         AS OPERATING_PROFIT_MARGIN,
            TRY_TO_NUMBER(PRETAXPROFITMARGIN, 38, 6)                            AS PRETAX_PROFIT_MARGIN,
            TRY_TO_NUMBER(NETPROFITMARGIN, 38, 6)                               AS NET_PROFIT_MARGIN,
            TRY_TO_NUMBER(BOTTOMLINEPROFITMARGIN, 38, 6)                        AS BOTTOM_LINE_PROFIT_MARGIN,
            TRY_TO_NUMBER(RECEIVABLESTURNOVER, 38, 6)                           AS RECEIVABLES_TURNOVER,
            TRY_TO_NUMBER(PAYABLESTURNOVER, 38, 6)                              AS PAYABLES_TURNOVER,
            TRY_TO_NUMBER(INVENTORYTURNOVER, 38, 6)                             AS INVENTORY_TURNOVER,
            TRY_TO_NUMBER(FIXEDASSETTURNOVER, 38, 6)                            AS FIXED_ASSET_TURNOVER,
            TRY_TO_NUMBER(ASSETTURNOVER, 38, 6)                                 AS ASSET_TURNOVER,
            TRY_TO_NUMBER(CURRENTRATIO, 38, 6)                                  AS CURRENT_RATIO,
            TRY_TO_NUMBER(QUICKRATIO, 38, 6)                                    AS QUICK_RATIO,
            TRY_TO_NUMBER(SOLVENCYRATIO, 38, 6)                                 AS SOLVENCY_RATIO,
            TRY_TO_NUMBER(CASHRATIO, 38, 6)                                     AS CASH_RATIO,
            TRY_TO_NUMBER(PRICETOEARNINGSRATIO, 38, 6)                          AS PE_RATIO,
            TRY_TO_NUMBER(PRICETOEARNINGSGROWTHRATIO, 38, 6)                    AS PEG_RATIO,
            TRY_TO_NUMBER(PRICETOBOOKRATIO, 38, 6)                              AS PB_RATIO,
            TRY_TO_NUMBER(PRICETOSALESRATIO, 38, 6)                             AS PS_RATIO,
            TRY_TO_NUMBER(PRICETOFREECASHFLOWRATIO, 38, 6)                      AS P_TO_FCF,
            TRY_TO_NUMBER(PRICETOOPERATINGCASHFLOWRATIO, 38, 6)                 AS P_TO_OCF,
            TRY_TO_NUMBER(DEBTTOASSETSRATIO, 38, 6)                              AS DEBT_TO_ASSETS,
            TRY_TO_NUMBER(DEBTTOEQUITYRATIO, 38, 6)                             AS DEBT_TO_EQUITY,
            TRY_TO_NUMBER(DEBTTOCAPITALRATIO, 38, 6)                            AS DEBT_TO_CAPITAL,
            TRY_TO_NUMBER(LONGTERMDEBTTOCAPITALRATIO, 38, 6)                     AS LT_DEBT_TO_CAPITAL,
            TRY_TO_NUMBER(FINANCIALLEVERAGERATIO, 38, 6)                         AS FINANCIAL_LEVERAGE,
            TRY_TO_NUMBER(OPERATINGCASHFLOWRATIO, 38, 6)                        AS OCF_RATIO,
            TRY_TO_NUMBER(OPERATINGCASHFLOWSALESRATIO, 38, 6)                   AS OCF_TO_SALES,
            TRY_TO_NUMBER(FREECASHFLOWOPERATINGCASHFLOWRATIO, 38, 6)            AS FCF_TO_OCF,
            TRY_TO_NUMBER(DEBTSERVICECOVERAGERATIO, 38, 6)                      AS DSCR,
            TRY_TO_NUMBER(INTERESTCOVERAGERATIO, 38, 6)                         AS INTEREST_COVERAGE,
            TRY_TO_NUMBER(DIVIDENDPAYOUTRATIO, 38, 6)                           AS DIVIDEND_PAYOUT_RATIO,
            TRY_TO_NUMBER(DIVIDENDYIELD, 38, 6)                                 AS DIVIDEND_YIELD,
            TRY_TO_NUMBER(REVENUEPERSHARE, 38, 6)                               AS REVENUE_PER_SHARE,
            TRY_TO_NUMBER(NETINCOMEPERSHARE, 38, 6)                             AS NET_INCOME_PER_SHARE,
            TRY_TO_NUMBER(CASHPERSHARE, 38, 6)                                  AS CASH_PER_SHARE,
            TRY_TO_NUMBER(BOOKVALUEPERSHARE, 38, 6)                             AS BOOK_VALUE_PER_SHARE,
            TRY_TO_NUMBER(TANGIBLEBOOKVALUEPERSHARE, 38, 6)                     AS TANGIBLE_BOOK_VALUE_PER_SHARE,
            TRY_TO_NUMBER(OPERATINGCASHFLOWPERSHARE, 38, 6)                     AS OCF_PER_SHARE,
            TRY_TO_NUMBER(FREECASHFLOWPERSHARE, 38, 6)                          AS FCF_PER_SHARE,
            TRY_TO_NUMBER(EFFECTIVETAXRATE, 38, 6)                              AS EFFECTIVE_TAX_RATE,
            TRY_TO_NUMBER(ENTERPRISEVALUEMULTIPLE, 38, 6)                       AS EV_MULTIPLE,
            TRY_TO_NUMBER(DIVIDENDPERSHARE, 38, 6)                              AS DIVIDEND_PER_SHARE
        FROM VALUFLOW.RAW.RATIOS_ANNUAL
    """),

    ("RATIOS_QUARTERLY", "RAW.RATIOS_QUARTERLY", """
        CREATE OR REPLACE TABLE VALUFLOW.STAGING.RATIOS_QUARTERLY AS
        SELECT
            TICKER,
            TRY_TO_DATE(FMP_DATE)                                               AS PERIOD_DATE,
            FISCALYEAR,
            PERIOD,
            REPORTEDCURRENCY,
            TRY_TO_NUMBER(GROSSPROFITMARGIN, 38, 6)                             AS GROSS_PROFIT_MARGIN,
            TRY_TO_NUMBER(EBITMARGIN, 38, 6)                                    AS EBIT_MARGIN,
            TRY_TO_NUMBER(EBITDAMARGIN, 38, 6)                                  AS EBITDA_MARGIN,
            TRY_TO_NUMBER(OPERATINGPROFITMARGIN, 38, 6)                         AS OPERATING_PROFIT_MARGIN,
            TRY_TO_NUMBER(PRETAXPROFITMARGIN, 38, 6)                            AS PRETAX_PROFIT_MARGIN,
            TRY_TO_NUMBER(NETPROFITMARGIN, 38, 6)                               AS NET_PROFIT_MARGIN,
            TRY_TO_NUMBER(BOTTOMLINEPROFITMARGIN, 38, 6)                        AS BOTTOM_LINE_PROFIT_MARGIN,
            TRY_TO_NUMBER(RECEIVABLESTURNOVER, 38, 6)                           AS RECEIVABLES_TURNOVER,
            TRY_TO_NUMBER(PAYABLESTURNOVER, 38, 6)                              AS PAYABLES_TURNOVER,
            TRY_TO_NUMBER(INVENTORYTURNOVER, 38, 6)                             AS INVENTORY_TURNOVER,
            TRY_TO_NUMBER(FIXEDASSETTURNOVER, 38, 6)                            AS FIXED_ASSET_TURNOVER,
            TRY_TO_NUMBER(ASSETTURNOVER, 38, 6)                                 AS ASSET_TURNOVER,
            TRY_TO_NUMBER(CURRENTRATIO, 38, 6)                                  AS CURRENT_RATIO,
            TRY_TO_NUMBER(QUICKRATIO, 38, 6)                                    AS QUICK_RATIO,
            TRY_TO_NUMBER(SOLVENCYRATIO, 38, 6)                                 AS SOLVENCY_RATIO,
            TRY_TO_NUMBER(CASHRATIO, 38, 6)                                     AS CASH_RATIO,
            TRY_TO_NUMBER(PRICETOEARNINGSRATIO, 38, 6)                          AS PE_RATIO,
            TRY_TO_NUMBER(PRICETOEARNINGSGROWTHRATIO, 38, 6)                    AS PEG_RATIO,
            TRY_TO_NUMBER(PRICETOBOOKRATIO, 38, 6)                              AS PB_RATIO,
            TRY_TO_NUMBER(PRICETOSALESRATIO, 38, 6)                             AS PS_RATIO,
            TRY_TO_NUMBER(PRICETOFREECASHFLOWRATIO, 38, 6)                      AS P_TO_FCF,
            TRY_TO_NUMBER(PRICETOOPERATINGCASHFLOWRATIO, 38, 6)                 AS P_TO_OCF,
            TRY_TO_NUMBER(DEBTTOASSETSRATIO, 38, 6)                              AS DEBT_TO_ASSETS,
            TRY_TO_NUMBER(DEBTTOEQUITYRATIO, 38, 6)                             AS DEBT_TO_EQUITY,
            TRY_TO_NUMBER(DEBTTOCAPITALRATIO, 38, 6)                            AS DEBT_TO_CAPITAL,
            TRY_TO_NUMBER(LONGTERMDEBTTOCAPITALRATIO, 38, 6)                     AS LT_DEBT_TO_CAPITAL,
            TRY_TO_NUMBER(FINANCIALLEVERAGERATIO, 38, 6)                         AS FINANCIAL_LEVERAGE,
            TRY_TO_NUMBER(OPERATINGCASHFLOWRATIO, 38, 6)                        AS OCF_RATIO,
            TRY_TO_NUMBER(OPERATINGCASHFLOWSALESRATIO, 38, 6)                   AS OCF_TO_SALES,
            TRY_TO_NUMBER(FREECASHFLOWOPERATINGCASHFLOWRATIO, 38, 6)            AS FCF_TO_OCF,
            TRY_TO_NUMBER(DEBTSERVICECOVERAGERATIO, 38, 6)                      AS DSCR,
            TRY_TO_NUMBER(INTERESTCOVERAGERATIO, 38, 6)                         AS INTEREST_COVERAGE,
            TRY_TO_NUMBER(DIVIDENDPAYOUTRATIO, 38, 6)                           AS DIVIDEND_PAYOUT_RATIO,
            TRY_TO_NUMBER(DIVIDENDYIELD, 38, 6)                                 AS DIVIDEND_YIELD,
            TRY_TO_NUMBER(REVENUEPERSHARE, 38, 6)                               AS REVENUE_PER_SHARE,
            TRY_TO_NUMBER(NETINCOMEPERSHARE, 38, 6)                             AS NET_INCOME_PER_SHARE,
            TRY_TO_NUMBER(CASHPERSHARE, 38, 6)                                  AS CASH_PER_SHARE,
            TRY_TO_NUMBER(BOOKVALUEPERSHARE, 38, 6)                             AS BOOK_VALUE_PER_SHARE,
            TRY_TO_NUMBER(TANGIBLEBOOKVALUEPERSHARE, 38, 6)                     AS TANGIBLE_BOOK_VALUE_PER_SHARE,
            TRY_TO_NUMBER(OPERATINGCASHFLOWPERSHARE, 38, 6)                     AS OCF_PER_SHARE,
            TRY_TO_NUMBER(FREECASHFLOWPERSHARE, 38, 6)                          AS FCF_PER_SHARE,
            TRY_TO_NUMBER(EFFECTIVETAXRATE, 38, 6)                              AS EFFECTIVE_TAX_RATE,
            TRY_TO_NUMBER(ENTERPRISEVALUEMULTIPLE, 38, 6)                       AS EV_MULTIPLE,
            TRY_TO_NUMBER(DIVIDENDPERSHARE, 38, 6)                              AS DIVIDEND_PER_SHARE
        FROM VALUFLOW.RAW.RATIOS_QUARTERLY
    """),

    ("PRICE_DATA", "RAW.PRICE_DATA", """
        CREATE OR REPLACE TABLE VALUFLOW.STAGING.PRICE_DATA AS
        SELECT
            TICKER,
            TRY_TO_DATE(DATE)               AS PRICE_DATE,
            TRY_TO_NUMBER(ADJOPEN, 38, 6)   AS ADJ_OPEN,
            TRY_TO_NUMBER(ADJHIGH, 38, 6)   AS ADJ_HIGH,
            TRY_TO_NUMBER(ADJLOW, 38, 6)    AS ADJ_LOW,
            TRY_TO_NUMBER(ADJCLOSE, 38, 6)  AS ADJ_CLOSE,
            TRY_TO_NUMBER(VOLUME, 38, 2)    AS VOLUME
        FROM VALUFLOW.RAW.PRICE_DATA
    """),

    ("SHARES_OUTSTANDING", "RAW.SHARES_OUTSTANDING", """
        CREATE OR REPLACE TABLE VALUFLOW.STAGING.SHARES_OUTSTANDING AS
        SELECT
            TICKER,
            SHARES_OUTSTANDING,
            MARKET_CAP,
            PRICE,
            TRY_TO_DATE(PULLED_DATE) AS PULLED_DATE
        FROM VALUFLOW.RAW.SHARES_OUTSTANDING
    """),
]


def run():
    print("=" * 60)
    print("ValuFlow -- staging_v4.py")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    conn = snowflake.connector.connect(**SNOWFLAKE_CONN)
    cursor = conn.cursor()

    cursor.execute("CREATE SCHEMA IF NOT EXISTS VALUFLOW.STAGING")
    print("STAGING schema ready\n")

    success_count = 0
    error_count = 0

    for target_table, source_table, sql in STAGING_TABLES:
        print(f"  Building STAGING.{target_table} from {source_table}...")
        try:
            cursor.execute(sql)
            cursor.execute(f"SELECT COUNT(*) FROM VALUFLOW.STAGING.{target_table}")
            row_count = cursor.fetchone()[0]
            print(f"  {target_table} -- {row_count:,} rows")
            success_count += 1
        except Exception as e:
            print(f"  {target_table} -- ERROR: {e}")
            error_count += 1

    conn.close()

    print("\n" + "=" * 60)
    print("Staging complete")
    print(f"  Success: {success_count}")
    print(f"  Errors:  {error_count}")
    print(f"  Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    print("\nNext step: build regression beta model")


if __name__ == "__main__":
    run()