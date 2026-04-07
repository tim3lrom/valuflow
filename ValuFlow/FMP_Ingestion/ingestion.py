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

# --- TICKERS --- CHANGE THESE FOR DIFFERENT INDUSTRIES ---
TICKERS = ["VSAT", "IRDM", "GSAT"]

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

# --- MAIN INGESTION FUNCTION ---
def run():
    income_statements = []
    balance_sheets = []
    cash_flows = []
    price_data = []
    profiles = []
    key_metrics = []
    ratios = []

    for ticker in TICKERS:
        print(f"\nFetching data for {ticker}...")

        # Income Statement
        df_is = fetch_fmp("income-statement", ticker, "&period=annual&limit=5")
        income_statements.append(df_is)

        # Balance Sheet
        df_bs = fetch_fmp("balance-sheet-statement", ticker, "&period=annual&limit=5")
        balance_sheets.append(df_bs)

        # Cash Flow Statement
        df_cf = fetch_fmp("cash-flow-statement", ticker, "&period=annual&limit=5")
        cash_flows.append(df_cf)

        # Historical Price Data
        df_price = fetch_fmp("historical-price-eod/light", ticker, "&from=2020-01-01")
        price_data.append(df_price)

        # Company Profile
        df_profile = fetch_fmp("profile", ticker)
        profiles.append(df_profile)

        # Key Metrics
        df_km = fetch_fmp("key-metrics", ticker, "&period=annual&limit=5")
        key_metrics.append(df_km)

        # Financial Ratios
        df_ratios = fetch_fmp("ratios", ticker, "&period=annual&limit=5")
        ratios.append(df_ratios)

    # --- COMBINE ALL TICKERS ---
    df_is_all = pd.concat(income_statements, ignore_index=True)
    df_bs_all = pd.concat(balance_sheets, ignore_index=True)
    df_cf_all = pd.concat(cash_flows, ignore_index=True)
    df_price_all = pd.concat(price_data, ignore_index=True)
    df_profile_all = pd.concat(profiles, ignore_index=True)
    df_km_all = pd.concat(key_metrics, ignore_index=True)
    df_ratios_all = pd.concat(ratios, ignore_index=True)

    print("\nUploading to Snowflake RAW...")

    # --- UPLOAD TO SNOWFLAKE RAW SCHEMA ---
    upload_to_snowflake(df_is_all, "INCOME_STMT", "RAW")
    upload_to_snowflake(df_bs_all, "BALANCE_SHEET", "RAW")
    upload_to_snowflake(df_cf_all, "CASH_FLOW_STMT", "RAW")
    upload_to_snowflake(df_price_all, "PRICE_DATA", "RAW")
    upload_to_snowflake(df_profile_all, "COMPANY_PROFILE", "RAW")
    upload_to_snowflake(df_km_all, "KEY_METRICS", "RAW")
    upload_to_snowflake(df_ratios_all, "RATIOS", "RAW")

    print("\nIngestion complete.")

if __name__ == "__main__":
    run()