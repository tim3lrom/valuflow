# ------- ValuFlow - Ticker Universe Builder ------- #
# ------- universe.py ------- #

# -- PURPOSE --
# -- Pulls all qualifying US equity tickers from FMP Stock Screener
# -- Stores results in VALUFLOW.RAW.TICKER_UNIVERSE as the master company list
# -- All downstream scripts (ingestion, equity_data, staging) read from this table
# -- Run this script once before any ingestion

# -- FILTERS APPLIED --
# -- Exchange:    NYSE and NASDAQ only
# -- Market Cap:  >= $1 billion (eliminates micro-caps and shell companies)
# -- Country:     US only
# -- Active:      Actively trading only
# -- Type:        Equities only — no ETFs, no mutual funds

# -- TABLE WRITTEN --
# -- VALUFLOW.RAW.TICKER_UNIVERSE -> master company list with sector, industry, exchange metadata

# -- NOTES --
# -- overwrite=True on write — full refresh each run
# -- FMP screener returns up to 10,000 results — limit set high to capture full universe
# -- Run this script before ingestion_v3.py, equity_data_v2.py, and staging_v4.py

import requests
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from cryptography.hazmat.primitives import serialization
from dotenv import load_dotenv
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
# 1. PULL TICKER UNIVERSE FROM FMP SCREENER
# ============================================================
def fetch_universe():
    print("Fetching ticker universe from FMP screener...")

    url = (
        f"{BASE_URL}/company-screener"
        f"?exchange=NYSE,NASDAQ"
        f"&marketCapMoreThan=1000000000"
        f"&country=US"
        f"&isActivelyTrading=true"
        f"&isEtf=false"
        f"&isFund=false"
        f"&limit=10000"
        f"&apikey={FMP_API_KEY}"
    )

    response = requests.get(url)
    data = response.json()

    if not isinstance(data, list) or len(data) == 0:
        print("ERROR: No data returned from screener")
        return pd.DataFrame()

    df = pd.DataFrame(data)

    # -- Keep only relevant columns
    keep_cols = [
        "symbol", "companyName", "exchange", "sector",
        "industry", "marketCap", "price", "country"
    ]
    keep_cols = [c for c in keep_cols if c in df.columns]
    df = df[keep_cols]

    # -- Rename for Snowflake consistency
    df = df.rename(columns={
        "symbol":       "ticker",
        "companyName":  "company_name",
        "exchange":     "exchange",
        "sector":       "sector",
        "industry":     "industry",
        "marketCap":    "market_cap",
        "price":        "price",
        "country":      "country"
    })

    # -- Add ingestion status column — used by checkpoint logic in ingestion_v3.py
    df["fundamentals_ingested"] = False
    df["price_ingested"] = False

    print(f"  {len(df)} tickers returned from screener")
    print(f"  Exchanges: {df['exchange'].value_counts().to_dict()}")
    print(f"  Sectors: {df['sector'].nunique()} unique sectors")

    return df

# ============================================================
# 2. UPLOAD TO SNOWFLAKE
# ============================================================
def upload_universe(df):
    if df.empty:
        print("ERROR: Empty DataFrame — nothing to upload")
        return

    conn = snowflake.connector.connect(**SNOWFLAKE_CONN)
    conn.cursor().execute("CREATE SCHEMA IF NOT EXISTS RAW")
    conn.cursor().execute("USE SCHEMA RAW")

    df.columns = [col.upper() for col in df.columns]
    df = df.reset_index(drop=True)

    success, nchunks, nrows, _ = write_pandas(
        conn, df, "TICKER_UNIVERSE",
        schema="RAW",
        auto_create_table=True,
        overwrite=True
    )

    print(f"\nUploaded {nrows} tickers to VALUFLOW.RAW.TICKER_UNIVERSE")
    conn.close()

# ============================================================
# 3. MAIN
# ============================================================
def run():
    df = fetch_universe()

    if df.empty:
        print("Exiting — no tickers to upload")
        return

    # -- Preview before uploading
    print(f"\n--- Universe Preview ---")
    print(df.head(10).to_string(index=False))
    print(f"\nTotal tickers: {len(df)}")

    upload_universe(df)
    print("\nUniverse build complete.")
    print("Next step: run ingestion_v3.py to pull financial data for all tickers")

if __name__ == "__main__":
    run()