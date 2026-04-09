# ------- ValuFlow - Staging Layer ------- #
# ------- staging_v3.py ------- #

# -- CHANGES FROM PREVIOUS VERSION --
# -- Previous Version: staging_v2.py
# -- Added TICKERS filter to all RAW queries — only stages specified tickers
# -- Moved SQLAlchemy engine to module level — single instance reused across all reads
# -- Replaced hardcoded TICKERS list with Section 0 user input (comma separated string)
# -- Added empty DataFrame guard to calculate_returns()

# -- PURPOSE --
# -- Transforms raw financial data from VALUFLOW.RAW into clean, model-ready tables
# -- Outputs to VALUFLOW.STAGING schema for downstream consumption by valuation models

# -- PIPELINE ORDER --
# -- Step 1: ingestion_v2.py  -> populates VALUFLOW.RAW financial statement tables
# -- Step 2: equity_data.py   -> populates VALUFLOW.RAW.PRICE_DATA and SHARES_OUTSTANDING
# -- Step 3: staging_v3.py    -> transforms RAW into STAGING (this script)

# -- TABLES WRITTEN --
# -- VALUFLOW.STAGING.FINANCIALS_CLEAN    -> income stmt, balance sheet, cash flow + derived fields
# -- VALUFLOW.STAGING.RETURNS             -> monthly stock returns per ticker
# -- VALUFLOW.STAGING.GROWTH_RATES        -> YoY growth rates for key financial metrics
# -- VALUFLOW.STAGING.MARGINS             -> margin calculations as % of revenue
# -- VALUFLOW.STAGING.VALUATION_INPUTS    -> ratios and inputs used by valuation models
# -- VALUFLOW.STAGING.BENCHMARK           -> S&P 500 monthly returns for beta regression

# -- DERIVED METRICS --
# -- FINANCIALS_CLEAN:   total_debt, de_ratio, net_debt, tax_rate
# -- GROWTH_RATES:       revenue_growth, ebitda_growth, netincome_growth, fcf_growth, operatingcashflow_growth
# -- MARGINS:            gross_margin, ebitda_margin, ebit_margin, net_margin, fcf_margin, operating_cashflow_margin
# -- VALUATION_INPUTS:   interest_coverage, debt_to_ebitda, debt_to_fcf, capex_to_revenue, capex_to_ocf, implied_coupon_rate

# -- NOTES --
# -- overwrite=True on all writes — full refresh each run, no duplicates
# -- Benchmark pull has no ticker filter — S&P 500 is market-wide data
# -- implied_coupon_rate = Interest Expense / Total Debt — falls back to NaN where Total Debt = 0
# -- Private key auth used for both read (SQLAlchemy) and write (snowflake.connector) connections

import requests
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from cryptography.hazmat.primitives import serialization
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
from dotenv import load_dotenv
import os

# --- LOAD ENVIRONMENT VARIABLES ---
load_dotenv(dotenv_path=r"C:\Users\timel\Desktop\ValuFlow\.env", override=True)

# --- FMP API KEY ---
FMP_API_KEY = os.getenv("FMP_API_KEY")
BASE_URL = "https://financialmodelingprep.com/stable"

# ============================================================
# 0. USER INPUTS
# ============================================================
# Enter tickers to stage separated by commas
# Example: "GSAT, KO, IRDM" for multiple or "GSAT" for a single ticker
# Tickers must already exist in VALUFLOW.RAW (run ingestion_v2.py and equity_data.py first)
TICKERS = [t.strip().upper() for t in "PG, CL, KMB, GIS".split(",")]

# --- TICKER FILTER HELPER ---
# Builds SQL IN clause from TICKERS list
# e.g. ["GSAT", "KO"] -> "('GSAT', 'KO')"
TICKER_FILTER = "('" + "', '".join(TICKERS) + "')"

# --- LOAD PRIVATE KEY ---
private_key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
with open(private_key_path, "rb") as key_file:
    private_key = serialization.load_pem_private_key(
        key_file.read(),
        password=None,
    )

private_key_bytes = private_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption()
)

# --- SNOWFLAKE CONNECTION DICT (for write_pandas) ---
SNOWFLAKE_CONN = {
    "user": os.getenv("SNOWFLAKE_USER"),
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": os.getenv("SNOWFLAKE_DATABASE"),
    "private_key": private_key_bytes,
    "role": "SYSADMIN"
}

# --- SQLALCHEMY ENGINE (for pd.read_sql) ---
# Created once at module level and reused across all reads
# Prevents a new engine being created for every query
engine = create_engine(URL(
    user=os.getenv("SNOWFLAKE_USER"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    role="SYSADMIN",
), connect_args={"private_key": private_key_bytes})

# --- READ FROM SNOWFLAKE ---
def read_from_snowflake(query):
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    return df

# --- UPLOAD TO SNOWFLAKE ---
def upload_to_snowflake(df, table_name, schema):
    if df.empty:
        print(f"Skipping upload for {table_name} — empty DataFrame")
        return
    conn = snowflake.connector.connect(**SNOWFLAKE_CONN)
    conn.cursor().execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    conn.cursor().execute(f"USE SCHEMA {schema}")
    df.columns = [col.upper() for col in df.columns]
    df = df.reset_index(drop=True)
    success, nchunks, nrows, _ = write_pandas(
        conn, df, table_name.upper(),
        schema=schema.upper(),
        auto_create_table=True,
        overwrite=True
    )
    print(f"Uploaded {nrows} rows to {schema}.{table_name}")
    conn.close()

# --- STEP 1: CALCULATE MONTHLY RETURNS FROM PRICE DATA ---
def calculate_returns():
    print("\nCalculating monthly returns...")
    df = read_from_snowflake(f"""
        SELECT SYMBOL, DATE, PRICE
        FROM VALUFLOW.RAW.PRICE_DATA
        WHERE SYMBOL IN {TICKER_FILTER}
    """)
    df.columns = df.columns.str.lower()

    if df.empty:
        print("  WARNING: No price data found for specified tickers — skipping returns calculation")
        return pd.DataFrame()

    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values(["symbol", "date"])

    df.set_index("date", inplace=True)
    monthly = (
        df.groupby("symbol")["price"]
        .resample("ME")
        .last()
        .reset_index()
    )

    monthly["return"] = monthly.groupby("symbol")["price"].pct_change()
    monthly = monthly.dropna(subset=["return"])
    monthly.rename(columns={"symbol": "ticker"}, inplace=True)

    print(f"  {len(monthly)} monthly return rows calculated")
    return monthly

# --- STEP 2: CALCULATE CORE FINANCIALS ---
def calculate_financials():
    print("\nCalculating core financials...")

    df_bs = read_from_snowflake(f"""
        SELECT SYMBOL, DATE, FISCALYEAR,
               LONGTERMDEBT, SHORTTERMDEBT,
               TOTALSTOCKHOLDERSEQUITY,
               CASHANDCASHEQUIVALENTS
        FROM VALUFLOW.RAW.BALANCE_SHEET
        WHERE SYMBOL IN {TICKER_FILTER}
    """)

    df_is = read_from_snowflake(f"""
        SELECT SYMBOL, FISCALYEAR,
               REVENUE, GROSSPROFIT, EBITDA, EBIT,
               OPERATINGINCOME, NETINCOME,
               INCOMEBEFORETAX, INCOMETAXEXPENSE,
               INTERESTEXPENSE, DEPRECIATIONANDAMORTIZATION
        FROM VALUFLOW.RAW.INCOME_STMT
        WHERE SYMBOL IN {TICKER_FILTER}
    """)

    df_cf = read_from_snowflake(f"""
        SELECT SYMBOL, FISCALYEAR,
               NETCASHPROVIDEDBYOPERATINGACTIVITIES,
               CAPITALEXPENDITURE,
               FREECASHFLOW,
               NETDEBTISSUANCE,
               NETCOMMONSTOCKISSUANCE,
               COMMONDIVIDENDSPAID,
               INCOMETAXESPAID,
               INTERESTPAID
        FROM VALUFLOW.RAW.CASH_FLOW_STMT
        WHERE SYMBOL IN {TICKER_FILTER}
    """)

    df_bs.columns = df_bs.columns.str.lower()
    df_is.columns = df_is.columns.str.lower()
    df_cf.columns = df_cf.columns.str.lower()

    # --- BALANCE SHEET CALCS ---
    df_bs["total_debt"] = df_bs["longtermdebt"].fillna(0) + df_bs["shorttermdebt"].fillna(0)
    df_bs["de_ratio"] = df_bs["total_debt"] / df_bs["totalstockholdersequity"]
    df_bs["net_debt"] = df_bs["total_debt"] - df_bs["cashandcashequivalents"].fillna(0)

    # --- INCOME STATEMENT CALCS ---
    df_is["tax_rate"] = df_is["incometaxexpense"] / df_is["incomebeforetax"]
    df_is["tax_rate"] = df_is["tax_rate"].clip(0, 1)

    # --- MERGE ALL THREE ---
    df = pd.merge(
        df_bs[["symbol", "date", "fiscalyear",
               "total_debt", "de_ratio", "net_debt",
               "cashandcashequivalents", "totalstockholdersequity",
               "longtermdebt", "shorttermdebt"]],
        df_is[["symbol", "fiscalyear", "tax_rate",
               "revenue", "grossprofit", "ebitda", "ebit",
               "operatingincome", "netincome",
               "incomebeforetax", "incometaxexpense",
               "interestexpense", "depreciationandamortization"]],
        on=["symbol", "fiscalyear"]
    )

    df = pd.merge(
        df,
        df_cf[["symbol", "fiscalyear",
               "netcashprovidedbyoperatingactivities",
               "capitalexpenditure", "freecashflow",
               "netdebtissuance", "netcommonstockissuance",
               "commondividendspaid", "incometaxespaid", "interestpaid"]],
        on=["symbol", "fiscalyear"]
    )

    df.rename(columns={"symbol": "ticker"}, inplace=True)
    print(f"  {len(df)} core financial rows calculated")
    return df

# --- STEP 3: CALCULATE GROWTH RATES ---
def calculate_growth_rates(df_financials):
    print("\nCalculating growth rates...")
    df = df_financials.copy()
    df = df.sort_values(["ticker", "fiscalyear"])

    df["revenue_growth"] = df.groupby("ticker")["revenue"].pct_change()
    df["ebitda_growth"] = df.groupby("ticker")["ebitda"].pct_change()
    df["netincome_growth"] = df.groupby("ticker")["netincome"].pct_change()
    df["fcf_growth"] = df.groupby("ticker")["freecashflow"].pct_change()
    df["operatingcashflow_growth"] = df.groupby("ticker")["netcashprovidedbyoperatingactivities"].pct_change()

    df = df[["ticker", "date", "fiscalyear",
             "revenue_growth", "ebitda_growth",
             "netincome_growth", "fcf_growth",
             "operatingcashflow_growth"]]
    df = df.dropna(subset=["revenue_growth"])

    print(f"  {len(df)} growth rate rows calculated")
    return df

# --- STEP 4: CALCULATE MARGINS ---
def calculate_margins(df_financials):
    print("\nCalculating margins...")
    df = df_financials.copy()

    df["gross_margin"] = df["grossprofit"] / df["revenue"]
    df["ebitda_margin"] = df["ebitda"] / df["revenue"]
    df["ebit_margin"] = df["ebit"] / df["revenue"]
    df["net_margin"] = df["netincome"] / df["revenue"]
    df["fcf_margin"] = df["freecashflow"] / df["revenue"]
    df["operating_cashflow_margin"] = df["netcashprovidedbyoperatingactivities"] / df["revenue"]

    df = df[["ticker", "date", "fiscalyear",
             "gross_margin", "ebitda_margin", "ebit_margin",
             "net_margin", "fcf_margin", "operating_cashflow_margin"]]

    print(f"  {len(df)} margin rows calculated")
    return df

# --- STEP 5: CALCULATE VALUATION INPUTS ---
def calculate_valuation_inputs(df_financials):
    print("\nCalculating valuation inputs...")
    df = df_financials.copy()

    df["interest_coverage"] = df["ebit"] / df["interestexpense"].replace(0, float("nan"))
    df["debt_to_ebitda"] = df["total_debt"] / df["ebitda"].replace(0, float("nan"))
    df["debt_to_fcf"] = df["total_debt"] / df["freecashflow"].replace(0, float("nan"))
    df["capex_to_revenue"] = df["capitalexpenditure"].abs() / df["revenue"]
    df["capex_to_ocf"] = df["capitalexpenditure"].abs() / df["netcashprovidedbyoperatingactivities"].replace(0, float("nan"))

    # Implied coupon rate — used for Dmv estimation in Hamada's Equation model
    # Derived as Interest Expense / Total Debt — represents the average cost of debt from financial statements
    # Falls back to NaN where Total Debt = 0 to avoid division errors
    df["implied_coupon_rate"] = df["interestexpense"] / df["total_debt"].replace(0, float("nan"))

    df = df[["ticker", "date", "fiscalyear",
             "ebitda", "freecashflow",
             "net_debt", "total_debt", "cashandcashequivalents",
             "interest_coverage", "debt_to_ebitda", "debt_to_fcf",
             "capex_to_revenue", "capex_to_ocf",
             "implied_coupon_rate",
             "capitalexpenditure", "netcashprovidedbyoperatingactivities"]]

    print(f"  {len(df)} valuation input rows calculated")
    return df

# --- STEP 6: PULL S&P 500 BENCHMARK RETURNS VIA FMP ---
# No ticker filter — benchmark is market-wide data independent of TICKERS list
def calculate_benchmark():
    print("\nPulling S&P 500 benchmark returns via FMP...")
    url = f"{BASE_URL}/historical-price-eod/light?symbol=%5EGSPC&from=2020-01-01&apikey={FMP_API_KEY}"
    response = requests.get(url)
    data = response.json()

    if not isinstance(data, list) or len(data) == 0:
        print("WARNING: No benchmark data returned")
        return pd.DataFrame()

    df = pd.DataFrame(data)
    df.columns = df.columns.str.lower()
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")

    df.set_index("date", inplace=True)
    monthly = df["price"].resample("ME").last().reset_index()
    monthly["market_return"] = monthly["price"].pct_change()
    monthly = monthly.dropna(subset=["market_return"])
    monthly = monthly[["date", "market_return"]]

    print(f"  {len(monthly)} benchmark return rows calculated")
    return monthly

# --- MAIN STAGING FUNCTION ---
def run():
    print(f"Staging tickers: {TICKERS}")
    df_financials = calculate_financials()
    df_returns = calculate_returns()
    df_growth = calculate_growth_rates(df_financials)
    df_margins = calculate_margins(df_financials)
    df_valuation = calculate_valuation_inputs(df_financials)
    df_benchmark = calculate_benchmark()

    print("\nUploading to Snowflake STAGING...")
    upload_to_snowflake(df_returns, "RETURNS", "STAGING")
    upload_to_snowflake(df_financials, "FINANCIALS_CLEAN", "STAGING")
    upload_to_snowflake(df_growth, "GROWTH_RATES", "STAGING")
    upload_to_snowflake(df_margins, "MARGINS", "STAGING")
    upload_to_snowflake(df_valuation, "VALUATION_INPUTS", "STAGING")
    upload_to_snowflake(df_benchmark, "BENCHMARK", "STAGING")

    print("\nStaging complete.")

if __name__ == "__main__":
    run()