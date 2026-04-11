# ------- ValuFlow - Equity Market Data Ingestion v2 ------- #
# ------- equity_data_v2.py ------- #

# -- PURPOSE --
# -- Pulls equity market data from FMP API for all tickers in TICKER_UNIVERSE
# -- Handles price history and shares outstanding separately from financial statements
# -- Stores raw data in VALUFLOW.RAW schema for downstream use in beta and Emv calculations

# -- CHANGES FROM equity_data.py --
# -- Ticker list now driven by VALUFLOW.RAW.TICKER_UNIVERSE (not hardcoded)
# -- Added checkpoint logic — PRICE_INGESTED tracks per-ticker status
# --   'False'   = not yet attempted
# --   'True'    = successfully ingested
# --   'NO_DATA' = FMP returned no data — flagged for review
# -- Added PRICE_INGESTED_AT timestamp per ticker
# -- Full price history pulled via paginated loop from 1930-01-01
# --   Loop continues until batch returns less than 5000 rows
# --   KO tested to go back to 1962 requiring 4 batches
# -- Switched to dividend-adjusted endpoint for accurate adjusted close prices
# -- Delete-by-ticker before append — preserves all other tickers in table
# -- Replaced write_pandas with manual INSERT to avoid reserved word conflicts
# -- Single Snowflake connection per ticker shared across upload and checkpoint
# -- FMP fetching and Snowflake uploading fully separated per ticker
# -- Added per-ticker status updates to terminal for visibility while running

# -- TABLES WRITTEN TO SNOWFLAKE --
# -- VALUFLOW.RAW.PRICE_DATA          -> full daily dividend-adjusted price history
# -- VALUFLOW.RAW.SHARES_OUTSTANDING  -> shares outstanding per ticker (most recent)

# -- PIPELINE ORDER --
# -- Step 1: universe.py       -> builds TICKER_UNIVERSE
# -- Step 2: ingestion_v4.py   -> pulls financial statement data
# -- Step 3: equity_data_v2.py -> pulls price history and shares outstanding (this script)
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
BATCH_SIZE = 50
HISTORY_START = "1930-01-01"  # -- earliest possible date — FMP returns whatever exists
MAX_ROWS = 5000               # -- FMP row cap per request

# ============================================================
# 1. FMP FETCH FUNCTIONS
# ============================================================
def fetch_price_history(ticker):
    all_data = []
    batch_num = 0
    to_date = None  # -- None means pull to today on first batch

    while True:
        batch_num += 1

        # -- Build URL
        if to_date:
            url = (
                f"{BASE_URL}/historical-price-eod/dividend-adjusted"
                f"?symbol={ticker}&from={HISTORY_START}&to={to_date}&apikey={FMP_API_KEY}"
            )
        else:
            url = (
                f"{BASE_URL}/historical-price-eod/dividend-adjusted"
                f"?symbol={ticker}&from={HISTORY_START}&apikey={FMP_API_KEY}"
            )

        try:
            response = requests.get(url, timeout=60)
            data = response.json()

            if not isinstance(data, list) or len(data) == 0:
                break

            print(f"    Batch {batch_num}: {len(data)} rows | {data[-1]['date']} to {data[0]['date']}")
            all_data.extend(data)

            # -- If less than MAX_ROWS returned we have reached the beginning of history
            if len(data) < MAX_ROWS:
                break

            # -- Set to_date to oldest date in this batch for next iteration
            to_date = data[-1]["date"]

        except Exception as e:
            print(f"  ERROR fetching price history batch {batch_num} for {ticker}: {e}")
            break

    if not all_data:
        return pd.DataFrame()

    df = pd.DataFrame(all_data)
    df["TICKER"] = ticker
    df.columns = [col.upper() for col in df.columns]

    # -- Deduplicate and sort ascending by date
    df = df.drop_duplicates(subset=["DATE"]).sort_values("DATE").reset_index(drop=True)

    print(f"    Total: {len(df)} rows | {df['DATE'].iloc[0]} to {df['DATE'].iloc[-1]}")
    return df

def fetch_shares_outstanding(ticker):
    url = f"{BASE_URL}/profile?symbol={ticker}&apikey={FMP_API_KEY}"
    try:
        response = requests.get(url, timeout=30)
        data = response.json()
        if not isinstance(data, list) or len(data) == 0:
            return pd.DataFrame()

        profile = data[0]
        price = profile.get("price", 0)
        market_cap = profile.get("marketCap", 0)

        shares_outstanding = market_cap / price if price and price > 0 else None

        return pd.DataFrame([{
            "TICKER":             ticker,
            "SHARES_OUTSTANDING": shares_outstanding,
            "MARKET_CAP":         market_cap,
            "PRICE":              price,
            "PULLED_DATE":        datetime.today().strftime('%Y-%m-%d')
        }])
    except Exception as e:
        print(f"  ERROR fetching shares outstanding for {ticker}: {e}")
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

    try:
        cursor.execute("""
            ALTER TABLE VALUFLOW.RAW.TICKER_UNIVERSE
            ADD COLUMN PRICE_INGESTED VARCHAR(20)
        """)
    except:
        pass

    try:
        cursor.execute("""
            ALTER TABLE VALUFLOW.RAW.TICKER_UNIVERSE
            ADD COLUMN PRICE_INGESTED_AT TIMESTAMP
        """)
    except:
        pass

    cursor.execute("""
        SELECT TICKER
        FROM VALUFLOW.RAW.TICKER_UNIVERSE
        WHERE PRICE_INGESTED IS NULL
           OR PRICE_INGESTED = 'False'
           OR PRICE_INGESTED = 'false'
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
    df_prices = fetch_price_history(ticker)
    df_shares = fetch_shares_outstanding(ticker)

    if df_prices.empty and df_shares.empty:
        return False

    conn = snowflake.connector.connect(**SNOWFLAKE_CONN)
    try:
        if not df_prices.empty:
            upload_to_snowflake(conn, df_prices, "PRICE_DATA", ticker)

        if not df_shares.empty:
            upload_to_snowflake(conn, df_shares, "SHARES_OUTSTANDING", ticker)

        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        conn.cursor().execute(f"""
            UPDATE VALUFLOW.RAW.TICKER_UNIVERSE
            SET PRICE_INGESTED = 'True',
                PRICE_INGESTED_AT = '{timestamp}'
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
    print("ValuFlow — equity_data_v2.py")
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
        print(f"\n  [{i}/{len(tickers)}] {ticker}")
        try:
            success = ingest_ticker(ticker)

            if success:
                success_count += 1
            else:
                conn = snowflake.connector.connect(**SNOWFLAKE_CONN)
                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                conn.cursor().execute(f"""
                    UPDATE VALUFLOW.RAW.TICKER_UNIVERSE
                    SET PRICE_INGESTED = 'NO_DATA',
                        PRICE_INGESTED_AT = '{timestamp}'
                    WHERE TICKER = '{ticker}'
                """)
                conn.commit()
                conn.close()
                no_data_count += 1
                print(f"    NO_DATA")

        except Exception as e:
            error_count += 1
            print(f"    ERROR: {e}")

        if i % BATCH_SIZE == 0:
            print(f"\n--- Progress: {i}/{len(tickers)} ---")
            print(f"    Success: {success_count} | No Data: {no_data_count} | Errors: {error_count}")
            print(f"    Time: {datetime.now().strftime('%H:%M:%S')}\n")

    print("\n" + "=" * 60)
    print("Equity data ingestion complete")
    print(f"  Total processed: {i}")
    print(f"  Success:         {success_count}")
    print(f"  No Data:         {no_data_count}")
    print(f"  Errors:          {error_count}")
    print(f"  Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    print("\nNext step: run staging_v4.py")

if __name__ == "__main__":
    run()