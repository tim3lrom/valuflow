# ------- ValuFlow - Equity Market Data Ingestion ------- #
# ------- equity_data.py ------- #

# -- PURPOSE --
# -- Pulls equity market data from FMP API for all tracked tickers
# -- Handles price history and shares outstanding separately from financial statements
# -- Stores raw data in VALUFLOW.RAW schema for downstream use in Emv calculation
# -- Designed to run independently from ingestion_v2.py and staging.py

# -- DATA FLOW --
# -- Source:      FMP API (financialmodelingprep.com/stable)
# -- Destination: VALUFLOW.RAW schema in Snowflake
# -- Frequency:
# --   Price History:       Daily — run as needed for latest prices
# --   Shares Outstanding:  Quarterly — run after each earnings cycle

# -- TABLES WRITTEN TO SNOWFLAKE --
# -- VALUFLOW.RAW.PRICE_DATA          -> daily price and volume history from 2020-01-01
# -- VALUFLOW.RAW.SHARES_OUTSTANDING  -> shares outstanding per ticker (most recent)

# -- TICKERS --
# -- Defined in TICKERS list below
# -- Must match tickers used in ingestion_v2.py and staging.py

# -- DEPENDENCIES --
# -- Snowflake credentials loaded via .env (python-dotenv)
# -- Private key authentication used for Snowflake connection (PKCS8 format)
# -- FMP API key loaded via .env

# -- NOTES --
# -- DELETE + append logic used — deletes only rows for the current ticker before uploading
# -- This preserves data for all other tickers in the table
# -- Shares outstanding derived as marketCap / price (FMP stable profile endpoint
# --   does not return sharesOutstanding directly)
# -- Derivation is an approximation — actual shares may differ slightly due to
# --   rounding in FMP's marketCap calculation
# -- Since Emv = shares_outstanding * price = (marketCap / price) * price = marketCap,
# --   market_cap is stored directly and used as Emv in the model
# -- Price history sourced from FMP historical-price-eod/light endpoint
# -- Run ingestion_v2.py for financial statement data (income, balance sheet, cash flow)
# -- Run staging.py after ingestion_v2.py to transform raw data into model-ready tables

import requests
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from cryptography.hazmat.primitives import serialization
from dotenv import load_dotenv
import os
from datetime import datetime

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

# --- TICKERS --- MUST MATCH ingestion_v2.py ---
TICKERS = ["GIS"]

# --- FMP FETCH FUNCTION ---
def fetch_fmp(endpoint, ticker, extra_params=""):
    url = f"{BASE_URL}/{endpoint}?symbol={ticker}{extra_params}&apikey={FMP_API_KEY}"
    response = requests.get(url)
    data = response.json()
    if isinstance(data, list) and len(data) > 0:
        df = pd.DataFrame(data)
        df["ticker"] = ticker
        return df
    else:
        print(f"WARNING: No data returned for {ticker} at {endpoint}")
        return pd.DataFrame()

# --- SNOWFLAKE UPLOAD FUNCTION ---
def upload_to_snowflake(df, table_name, schema, ticker):
    if df.empty:
        print(f"Skipping upload for {table_name} — empty DataFrame")
        return
    conn = snowflake.connector.connect(**SNOWFLAKE_CONN)
    conn.cursor().execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    conn.cursor().execute(f"USE SCHEMA {schema}")
    df.columns = [col.upper() for col in df.columns]
    df = df.reset_index(drop=True)

    # -- Check if table exists before attempting DELETE
    # -- If table does not exist yet, skip DELETE and let write_pandas create it
    table_exists = conn.cursor().execute(f"""
        SELECT COUNT(*) 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = '{schema.upper()}' 
        AND TABLE_NAME = '{table_name.upper()}'
    """).fetchone()[0]

    if table_exists:
        # -- Delete existing rows for this ticker only — preserves all other tickers
        conn.cursor().execute(f"""
            DELETE FROM {schema}.{table_name}
            WHERE TICKER = '{ticker}'
        """)
        print(f"  Deleted existing {ticker} rows from {schema}.{table_name}")
    else:
        print(f"  Table {schema}.{table_name} does not exist yet — will be created")

    success, nchunks, nrows, _ = write_pandas(
        conn, df, table_name.upper(),
        schema=schema.upper(),
        auto_create_table=True,
        overwrite=False
    )
    print(f"  Uploaded {nrows} rows to {schema}.{table_name}")
    conn.close()

# --- PULL PRICE HISTORY ---
def pull_price_history(ticker):
    print(f"  Fetching price history for {ticker}...")
    df = fetch_fmp("historical-price-eod/light", ticker, "&from=2020-01-01")
    df['date'] = pd.to_datetime(df['date']).dt.date  # -- Standardize date format
    print(f"  {len(df)} price rows fetched")
    return df

# --- PULL SHARES OUTSTANDING ---
def pull_shares_outstanding(ticker):
    print(f"  Fetching shares outstanding for {ticker}...")
    df_profile = fetch_fmp("profile", ticker)
    if df_profile.empty:
        return pd.DataFrame()

    price = df_profile["price"].iloc[0]
    market_cap = df_profile["marketCap"].iloc[0]

    # -- Shares outstanding derived as marketCap / price
    # -- FMP stable profile endpoint does not return sharesOutstanding directly
    # -- Result is an approximation — actual shares outstanding may differ slightly
    # --   due to rounding in FMP's marketCap calculation
    # -- Since Emv = shares_outstanding * price = (marketCap / price) * price = marketCap,
    # --   market_cap is stored directly and used as Emv in the model
    shares_outstanding = market_cap / price if price and price > 0 else None

    return pd.DataFrame([{
        "ticker":             ticker,
        "shares_outstanding": shares_outstanding,
        "market_cap":         market_cap,
        "price":              price,
        "pulled_date":        datetime.today().strftime('%Y-%m-%d')
    }])

# --- MAIN FUNCTION ---
def run():
    for ticker in TICKERS:
        print(f"\n--- Processing {ticker} ---")
        df_prices = pull_price_history(ticker)
        df_shares = pull_shares_outstanding(ticker)

        print(f"\n  Uploading {ticker} to Snowflake RAW...")
        upload_to_snowflake(df_prices, "PRICE_DATA", "RAW", ticker)
        upload_to_snowflake(df_shares, "SHARES_OUTSTANDING", "RAW", ticker)

    print("\nEquity data ingestion complete.")

if __name__ == "__main__":
    run()