# ------- ValuFlow - Financial Statement Ingestion v4 ------- #
# ------- ingestion_v4.py ------- #

# -- PURPOSE --
# -- Pulls fundamental financial statement data from FMP API
# -- for all tickers in VALUFLOW.RAW.TICKER_UNIVERSE
# -- Stores raw data in VALUFLOW.RAW schema for downstream staging
# -- Checkpoint logic ensures crash recovery — resumes from last successful ticker

# -- SHOULD NOT BE RUN MORE THAN ONCE, THIS IS A MASTER PULL, TAKES IN ALL FINANCIAL HISTORY FOR ALL OF NYSE & NASDAQ

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
# -- Replaced quoted column approach with explicit rename map for reserved words
# -- FMP fetching and Snowflake uploading fully separated per ticker
# -- Single Snowflake connection per ticker shared across upload and checkpoint
# -- Removed verbose upload print statements for cleaner console output

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
# -- Step 2: ingestion_v4.py   -> pulls financial data (this script)
# -- Step 3: equity_data_v2.py -> pulls price history and shares outstanding
# -- Step 4: staging_v4.py     -> transforms RAW into STAGING

import requests
import pandas as pd
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from dotenv import load_dotenv
from datetime import datetime
import os

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
STATEMENT_LIMIT = 1000
BATCH_SIZE = 50

# ============================================================
# 0.1 COLUMN SANITIZATION
# ============================================================
RENAME_MAP = {
    "DATE":       "FMP_DATE",
    "VALUE":      "METRIC_VALUE",
    "NAME":       "COMPANY_NAME",
    "NUMBER":     "DOC_NUMBER",
    "TIME":       "REPORT_TIME",
    "TYPE":       "RECORD_TYPE",
    "LEVEL":      "RECORD_LEVEL",
    "SIZE":       "RECORD_SIZE",
    "OPTION":     "RECORD_OPTION",
    "ORDER":      "RECORD_ORDER",
    "GROUP":      "RECORD_GROUP",
    "FROM":       "RECORD_FROM",
    "IN":         "RECORD_IN",
    "ON":         "RECORD_ON",
    "AS":         "RECORD_AS",
    "IS":         "RECORD_IS",
    "NOT":        "RECORD_NOT",
    "AND":        "RECORD_AND",
    "OR":         "RECORD_OR",
    "END":        "RECORD_END",
    "CASE":       "RECORD_CASE",
    "WHEN":       "RECORD_WHEN",
    "THEN":       "RECORD_THEN",
    "ELSE":       "RECORD_ELSE",
    "INTERVAL":   "RECORD_INTERVAL",
    "PERCENT":    "RECORD_PERCENT",
    "POSITION":   "RECORD_POSITION",
    "TIMESTAMP":  "RECORD_TIMESTAMP",
    "LANGUAGE":   "RECORD_LANGUAGE",
    "YEAR":       "RECORD_YEAR",
    "MONTH":      "RECORD_MONTH",
    "DAY":        "RECORD_DAY",
    "HOUR":       "RECORD_HOUR",
    "MINUTE":     "RECORD_MINUTE",
    "SECOND":     "RECORD_SECOND",
    "ZONE":       "RECORD_ZONE",
    "ROLE":       "RECORD_ROLE",
    "SCHEMA":     "RECORD_SCHEMA",
    "TABLE":      "RECORD_TABLE",
    "COLUMN":     "RECORD_COLUMN",
    "INDEX":      "RECORD_INDEX",
    "VIEW":       "RECORD_VIEW",
    "SEQUENCE":   "RECORD_SEQUENCE",
    "FUNCTION":   "RECORD_FUNCTION",
    "SELECT":     "RECORD_SELECT",
    "WHERE":      "RECORD_WHERE",
    "JOIN":       "RECORD_JOIN",
    "LIMIT":      "RECORD_LIMIT",
    "OFFSET":     "RECORD_OFFSET",
    "HAVING":     "RECORD_HAVING"
}

def sanitize_columns(df):
    df.columns = [col.upper() for col in df.columns]
    df.columns = [RENAME_MAP.get(col, col) for col in df.columns]
    return df

# ============================================================
# 1. FMP FETCH FUNCTION
# ============================================================
def fetch_fmp(endpoint, ticker, extra_params=""):
    url = f"{BASE_URL}/{endpoint}?symbol={ticker}{extra_params}&apikey={FMP_API_KEY}"
    try:
        response = requests.get(url, timeout=30)
        data = response.json()

        if not isinstance(data, list):
            return pd.DataFrame()

        if len(data) > 0:
            df = pd.DataFrame(data)
            df["TICKER"] = ticker
            df = sanitize_columns(df)
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
    df = df.reset_index(drop=True)

    col_defs = ", ".join([f"{col} VARCHAR" for col in df.columns])
    full_table = f"VALUFLOW.RAW.{table_name}"

    cursor.execute(f"CREATE TABLE IF NOT EXISTS {full_table} ({col_defs})")

    try:
        cursor.execute(f"DELETE FROM {full_table} WHERE TICKER = %s", (ticker,))
    except:
        pass

    chunk_size = 1000
    cols = ", ".join(df.columns)

    for start in range(0, len(df), chunk_size):
        chunk = df.iloc[start:start + chunk_size]
        placeholders = ", ".join(["%s"] * len(df.columns))
        insert_sql = f"INSERT INTO {full_table} ({cols}) VALUES ({placeholders})"

        rows = [tuple(
            None if pd.isna(v) else str(v) if not isinstance(v, (int, float, bool)) else v
            for v in row
        ) for row in chunk.itertuples(index=False)]

        cursor.executemany(insert_sql, rows)

# ============================================================
# 3. LOAD TICKERS FROM TICKER_UNIVERSE
# ============================================================
def load_tickers():
    print("Loading tickers from VALUFLOW.RAW.TICKER_UNIVERSE...")

    conn = snowflake.connector.connect(**SNOWFLAKE_CONN)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT TICKER
        FROM VALUFLOW.RAW.TICKER_UNIVERSE
        WHERE FUNDAMENTALS_INGESTED IS NULL
           OR FUNDAMENTALS_INGESTED = 'False'
           OR FUNDAMENTALS_INGESTED = 'false'
        ORDER BY MARKET_CAP DESC
    """)

    tickers = [row[0] for row in cursor.fetchall()]
    conn.close()
    print(f"  {len(tickers)} tickers pending ingestion")
    return tickers

# ============================================================
# 4. INGEST SINGLE TICKER
# ============================================================
def ingest_ticker(ticker):
    # -- STEP 1: Fetch all FMP data first — no Snowflake connections open
    results = {}
    results["INCOME_STMT_ANNUAL"]      = fetch_fmp("income-statement", ticker, f"&period=annual&limit={STATEMENT_LIMIT}")
    results["BALANCE_SHEET_ANNUAL"]    = fetch_fmp("balance-sheet-statement", ticker, f"&period=annual&limit={STATEMENT_LIMIT}")
    results["CASH_FLOW_ANNUAL"]        = fetch_fmp("cash-flow-statement", ticker, f"&period=annual&limit={STATEMENT_LIMIT}")
    results["INCOME_STMT_QUARTERLY"]   = fetch_fmp("income-statement", ticker, f"&period=quarter&limit={STATEMENT_LIMIT}")
    results["BALANCE_SHEET_QUARTERLY"] = fetch_fmp("balance-sheet-statement", ticker, f"&period=quarter&limit={STATEMENT_LIMIT}")
    results["CASH_FLOW_QUARTERLY"]     = fetch_fmp("cash-flow-statement", ticker, f"&period=quarter&limit={STATEMENT_LIMIT}")
    results["KEY_METRICS_ANNUAL"]      = fetch_fmp("key-metrics", ticker, f"&period=annual&limit={STATEMENT_LIMIT}")
    results["KEY_METRICS_QUARTERLY"]   = fetch_fmp("key-metrics", ticker, f"&period=quarter&limit={STATEMENT_LIMIT}")
    results["RATIOS_ANNUAL"]           = fetch_fmp("ratios", ticker, f"&period=annual&limit={STATEMENT_LIMIT}")
    results["RATIOS_QUARTERLY"]        = fetch_fmp("ratios", ticker, f"&period=quarter&limit={STATEMENT_LIMIT}")
    results["COMPANY_PROFILE"]         = fetch_fmp("profile", ticker)

    all_empty = all(df.empty for df in results.values())
    if all_empty:
        return False

    # -- STEP 2: Single Snowflake connection for both upload and checkpoint
    conn = snowflake.connector.connect(**SNOWFLAKE_CONN)
    try:
        for table_name, df in results.items():
            if not df.empty:
                upload_to_snowflake(conn, df, table_name, ticker)

        # -- Update checkpoint in same connection
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        conn.cursor().execute(f"""
            UPDATE VALUFLOW.RAW.TICKER_UNIVERSE
            SET FUNDAMENTALS_INGESTED = 'True',
                FUNDAMENTALS_INGESTED_AT = '{timestamp}'
            WHERE TICKER = '{ticker}'
        """)
        conn.commit()
    finally:
        conn.close()

    return True

# ============================================================
# 5. MAIN
# ============================================================
def run():
    print("=" * 60)
    print("ValuFlow — ingestion_v4.py")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    tickers = load_tickers()

    if not tickers:
        print("All tickers already ingested. Exiting.")
        return

    success_count = 0
    no_data_count = 0
    error_count = 0

    for i, ticker in enumerate(tickers, 1):
        try:
            success = ingest_ticker(ticker)

            if success:
                success_count += 1
            else:
                # -- Mark NO_DATA in separate connection
                conn = snowflake.connector.connect(**SNOWFLAKE_CONN)
                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                conn.cursor().execute(f"""
                    UPDATE VALUFLOW.RAW.TICKER_UNIVERSE
                    SET FUNDAMENTALS_INGESTED = 'NO_DATA',
                        FUNDAMENTALS_INGESTED_AT = '{timestamp}'
                    WHERE TICKER = '{ticker}'
                """)
                conn.commit()
                conn.close()
                no_data_count += 1
                print(f"  [{i}/{len(tickers)}] {ticker} — NO_DATA")

        except Exception as e:
            error_count += 1
            print(f"  [{i}/{len(tickers)}] {ticker} — ERROR: {e}")

        if i % BATCH_SIZE == 0:
            print(f"\n--- Progress: {i}/{len(tickers)} ---")
            print(f"    Success: {success_count} | No Data: {no_data_count} | Errors: {error_count}")
            print(f"    Time: {datetime.now().strftime('%H:%M:%S')}\n")

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