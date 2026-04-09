# ------- ValuFlow - Financial Statement Ingestion v3 ------- #
# ------- ingestion_v4.py ------- #

# -- PURPOSE --
# -- Pulls fundamental financial statement data from FMP API
# -- for all tickers in VALUFLOW.RAW.TICKER_UNIVERSE
# -- Stores raw data in VALUFLOW.RAW schema for downstream staging
# -- Checkpoint logic ensures crash recovery — resumes from last successful ticker

# -- CHANGES FROM ingestion_v2.py --
# -- Ticker list now driven by VALUFLOW.RAW.TICKER_UNIVERSE (not hardcoded)
# -- Added checkpoint logic — FUNDAMENTALS_INGESTED tracks per-ticker status
# --   'False'   = not yet attempted
# --   'True'    = successfully ingested
# --   'NO_DATA' = FMP returned no data — flagged for review
# -- Added FUNDAMENTALS_INGESTED_AT timestamp per ticker
# -- Pulls annual AND quarterly for all statement types
# -- Delete-by-ticker before append — preserves all other tickers in table
# -- Replaced overwrite=True with ticker-aware delete + append

# -- TABLES WRITTEN --
# -- VALUFLOW.RAW.INCOME_STMT_ANNUAL
# -- VALUFLOW.RAW.INCOME_STMT_QUARTERLY
# -- VALUFLOW.RAW.BALANCE_SHEET_ANNUAL
# -- VALUFLOW.RAW.BALANCE_SHEET_QUARTERLY
# -- VALUFLOW.RAW.CASH_FLOW_ANNUAL
# -- VALUFLOW.RAW.CASH_FLOW_QUARTERLY
# -- VALUFLOW.RAW.KEY_METRICS_ANNUAL
# -- VALUFLOW.RAW.KEY_METRICS_QUARTERLY
# -- VALUFLOW.RAW.RATIOS_ANNUAL
# -- VALUFLOW.RAW.RATIOS_QUARTERLY
# -- VALUFLOW.RAW.COMPANY_PROFILE

# -- PIPELINE ORDER --
# -- Step 1: universe.py       -> builds TICKER_UNIVERSE
# -- Step 2: ingestion_v3.py   -> pulls financial data (this script)
# -- Step 3: equity_data_v2.py -> pulls price history and shares outstanding
# -- Step 4: staging_v4.py     -> transforms RAW into STAGING

import requests
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from cryptography.hazmat.primitives import serialization
from dotenv import load_dotenv
from datetime import datetime
import os
import time

# --- LOAD ENVIRONMENT VARIABLES ---
load_dotenv(dotenv_path=r"C:\Users\timel\Desktop\ValuFlow\.env", override=True)

# --- FMP API KEY ---
FMP_API_KEY = os.getenv("FMP_API_KEY")
BASE_URL = "https://financialmodelingprep.com/stable"

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

# --- SNOWFLAKE CONNECTION ---
SNOWFLAKE_CONN = {
    "user": os.getenv("SNOWFLAKE_USER"),
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": os.getenv("SNOWFLAKE_DATABASE"),
    "private_key": private_key_bytes,
    "role": "SYSADMIN"
}

# ============================================================
# 0. USER INPUTS
# ============================================================
# -- Limit for financial statement history — None = full history
STATEMENT_LIMIT = 1000      # effectively unlimited — FMP caps at available history
BATCH_SIZE = 50             # number of tickers to process before printing progress

# ============================================================
# 1. FMP FETCH FUNCTION
# ============================================================
def fetch_fmp(endpoint, ticker, extra_params=""):
    url = f"{BASE_URL}/{endpoint}?symbol={ticker}{extra_params}&apikey={FMP_API_KEY}"
    try:
        response = requests.get(url, timeout=30)
        data = response.json()
        if isinstance(data, list) and len(data) > 0:
            df = pd.DataFrame(data)
            df["TICKER"] = ticker
            return df
        else:
            return pd.DataFrame()
    except Exception as e:
        print(f"  ERROR fetching {endpoint} for {ticker}: {e}")
        return pd.DataFrame()

# ============================================================
# 2. SNOWFLAKE UPLOAD FUNCTION
# ============================================================
def upload_to_snowflake(conn, df, table_name, ticker):
    if df.empty:
        return

    cursor = conn.cursor()

    # -- Create table if it doesn't exist yet
    cursor.execute(f"CREATE TABLE IF NOT EXISTS RAW.{table_name} (TICKER VARCHAR)")

    # -- Delete existing rows for this ticker only
    cursor.execute(f"DELETE FROM RAW.{table_name} WHERE TICKER = '{ticker}'")

    # -- Standardize column names
    df.columns = [col.upper() for col in df.columns]
    df = df.reset_index(drop=True)

    write_pandas(
        conn, df, table_name,
        schema="RAW",
        auto_create_table=True,
        overwrite=False
    )

# ============================================================
# 3. LOAD TICKERS FROM TICKER_UNIVERSE
# ============================================================
def load_tickers(conn):
    print("Loading tickers from VALUFLOW.RAW.TICKER_UNIVERSE...")

    # -- Ensure FUNDAMENTALS_INGESTED column is VARCHAR to support 'NO_DATA'
    cursor = conn.cursor()
    try:
        cursor.execute("""
            ALTER TABLE RAW.TICKER_UNIVERSE
            ALTER COLUMN FUNDAMENTALS_INGESTED SET DATA TYPE VARCHAR(20)
        """)
    except:
        pass  # column may already be VARCHAR

    # -- Also add timestamp column if it doesn't exist
    try:
        cursor.execute("""
            ALTER TABLE RAW.TICKER_UNIVERSE
            ADD COLUMN IF NOT EXISTS FUNDAMENTALS_INGESTED_AT TIMESTAMP
        """)
    except:
        pass

    # -- Pull only tickers not yet successfully ingested
    cursor.execute("""
        SELECT TICKER
        FROM RAW.TICKER_UNIVERSE
        WHERE FUNDAMENTALS_INGESTED IS NULL
           OR FUNDAMENTALS_INGESTED = 'False'
           OR FUNDAMENTALS_INGESTED = 'false'
        ORDER BY MARKET_CAP DESC
    """)

    tickers = [row[0] for row in cursor.fetchall()]
    print(f"  {len(tickers)} tickers pending ingestion")
    return tickers

# ============================================================
# 4. UPDATE CHECKPOINT
# ============================================================
def update_checkpoint(conn, ticker, status):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    conn.cursor().execute(f"""
        UPDATE RAW.TICKER_UNIVERSE
        SET FUNDAMENTALS_INGESTED = '{status}',
            FUNDAMENTALS_INGESTED_AT = '{timestamp}'
        WHERE TICKER = '{ticker}'
    """)

# ============================================================
# 5. INGEST SINGLE TICKER
# ============================================================
def ingest_ticker(conn, ticker):
    results = {}

    # -- Annual statements
    results["INCOME_STMT_ANNUAL"]    = fetch_fmp("income-statement", ticker, f"&period=annual&limit={STATEMENT_LIMIT}")
    results["BALANCE_SHEET_ANNUAL"]  = fetch_fmp("balance-sheet-statement", ticker, f"&period=annual&limit={STATEMENT_LIMIT}")
    results["CASH_FLOW_ANNUAL"]      = fetch_fmp("cash-flow-statement", ticker, f"&period=annual&limit={STATEMENT_LIMIT}")

    # -- Quarterly statements
    results["INCOME_STMT_QUARTERLY"]   = fetch_fmp("income-statement", ticker, f"&period=quarter&limit={STATEMENT_LIMIT}")
    results["BALANCE_SHEET_QUARTERLY"] = fetch_fmp("balance-sheet-statement", ticker, f"&period=quarter&limit={STATEMENT_LIMIT}")
    results["CASH_FLOW_QUARTERLY"]     = fetch_fmp("cash-flow-statement", ticker, f"&period=quarter&limit={STATEMENT_LIMIT}")

    # -- Key metrics and ratios
    results["KEY_METRICS_ANNUAL"]      = fetch_fmp("key-metrics", ticker, f"&period=annual&limit={STATEMENT_LIMIT}")
    results["KEY_METRICS_QUARTERLY"]   = fetch_fmp("key-metrics", ticker, f"&period=quarter&limit={STATEMENT_LIMIT}")
    results["RATIOS_ANNUAL"]           = fetch_fmp("ratios", ticker, f"&period=annual&limit={STATEMENT_LIMIT}")
    results["RATIOS_QUARTERLY"]        = fetch_fmp("ratios", ticker, f"&period=quarter&limit={STATEMENT_LIMIT}")

    # -- Company profile
    results["COMPANY_PROFILE"] = fetch_fmp("profile", ticker)

    # -- Check if all results are empty
    all_empty = all(df.empty for df in results.values())
    if all_empty:
        return False, results

    # -- Upload non-empty results
    for table_name, df in results.items():
        if not df.empty:
            upload_to_snowflake(conn, df, table_name, ticker)

    return True, results

# ============================================================
# 6. MAIN
# ============================================================
def run():
    print("=" * 60)
    print("ValuFlow — ingestion_v3.py")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    conn = snowflake.connector.connect(**SNOWFLAKE_CONN)
    conn.cursor().execute("USE SCHEMA RAW")

    tickers = load_tickers(conn)

    if not tickers:
        print("All tickers already ingested. Exiting.")
        conn.close()
        return

    success_count = 0
    no_data_count = 0
    error_count = 0

    for i, ticker in enumerate(tickers, 1):
        try:
            success, _ = ingest_ticker(conn, ticker)

            if success:
                update_checkpoint(conn, ticker, "True")
                success_count += 1
            else:
                update_checkpoint(conn, ticker, "NO_DATA")
                no_data_count += 1
                print(f"  [{i}/{len(tickers)}] {ticker} — NO_DATA")

        except Exception as e:
            error_count += 1
            print(f"  [{i}/{len(tickers)}] {ticker} — ERROR: {e}")
            # -- Leave as False so it retries on next run

        # -- Progress update every BATCH_SIZE tickers
        if i % BATCH_SIZE == 0:
            print(f"\n--- Progress: {i}/{len(tickers)} tickers processed ---")
            print(f"    Success: {success_count} | No Data: {no_data_count} | Errors: {error_count}")
            print(f"    Time: {datetime.now().strftime('%H:%M:%S')}\n")

    conn.close()

    print("\n" + "=" * 60)
    print("Ingestion complete")
    print(f"  Total processed: {i}")
    print(f"  Success:         {success_count}")
    print(f"  No Data:         {no_data_count}")
    print(f"  Errors:          {error_count}")
    print(f"  Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    print("\nNext step: run equity_data_v2.py for price history")

if __name__ == "__main__":
    run()