# ------- ValuFlow - S&P 500 Price Backfill ------- #
# ------- sp500_ingest.py ------- #

# -- PURPOSE --
# -- Pulls full historical daily S&P 500 prices from FMP
# -- Paginates in date chunks to bypass FMP's 5,000 record per call limit
# -- Truncates and reloads VALUFLOW.RAW.SP500_PRICES
# -- Run once to backfill, then periodically to keep current

# -- TABLE WRITTEN --
# -- VALUFLOW.RAW.SP500_PRICES (truncate + reload)

# -- FMP ENDPOINT --
# -- https://financialmodelingprep.com/stable/historical-price-eod/light?symbol=^GSPC&from=...&to=...&apikey=...
# -- NOTE: FMP caps responses at 5,000 records per call (~20 years of trading days)
# --       Script chunks the date range to collect the full history

import os
import requests
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from cryptography.hazmat.primitives import serialization
from dotenv import load_dotenv
from datetime import datetime, date

load_dotenv(dotenv_path=r"C:\Users\timel\Desktop\ValuFlow\.env", override=True)

# ============================================================
# 0. USER INPUTS
# ============================================================
# -- Each chunk covers ~20 years of trading days (5,000 records)
# -- Add more chunks if you want to go back further than 1970
DATE_CHUNKS = [
    ("1970-01-01", "1989-12-31"),
    ("1990-01-01", "2006-12-31"),
    ("2007-01-01", "2026-12-31"),
]

# ============================================================
# 1. PRIVATE KEY AUTH
# ============================================================
private_key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
with open(private_key_path, "rb") as key_file:
    private_key = serialization.load_pem_private_key(key_file.read(), password=None)

private_key_bytes = private_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption()
)

SNOWFLAKE_CONN = {
    "user":        os.getenv("SNOWFLAKE_USER"),
    "account":     os.getenv("SNOWFLAKE_ACCOUNT"),
    "warehouse":   os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database":    "VALUFLOW",
    "schema":      "RAW",
    "private_key": private_key_bytes,
    "role":        "SYSADMIN",
    "network_timeout": 300,
    "login_timeout":   60,
}

FMP_API_KEY = os.getenv("FMP_API_KEY")

print("=" * 60)
print("ValuFlow -- sp500_ingest.py")
print(f"Started:  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Chunks:   {len(DATE_CHUNKS)}")
print("=" * 60)

# ============================================================
# 2. PULL ALL CHUNKS FROM FMP
# ============================================================
all_frames = []

for start, end in DATE_CHUNKS:
    url = (
        f"https://financialmodelingprep.com/stable/historical-price-eod/light"
        f"?symbol=%5EGSPC&from={start}&to={end}&apikey={FMP_API_KEY}"
    )
    print(f"\nFetching {start} to {end}...")
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    if not data:
        print(f"  No data returned for this chunk — skipping")
        continue

    df_chunk = pd.DataFrame(data)
    print(f"  Records returned: {len(df_chunk)}")

    if len(df_chunk) == 5000:
        print(f"  WARNING: Hit 5,000 record cap — consider splitting this chunk further")

    all_frames.append(df_chunk)

if not all_frames:
    print("\nNo data returned across all chunks. Check API key or date ranges.")
    exit()

# ============================================================
# 3. COMBINE & CLEAN
# ============================================================
df = pd.concat(all_frames, ignore_index=True)

df = df.rename(columns={
    "date":   "DATE",
    "price":  "PRICE",
    "volume": "VOLUME",
})

df["DATE"] = pd.to_datetime(df["DATE"]).dt.date

# -- Deduplicate in case chunks overlap on boundary dates
df = df.drop_duplicates(subset=["DATE"])
df = df[["DATE", "PRICE", "VOLUME"]].sort_values("DATE").reset_index(drop=True)

print(f"\nTotal rows after dedup: {len(df):,}")
print(f"Date range: {df['DATE'].min()} to {df['DATE'].max()}")
print(f"Sample (last 3 rows):")
print(df.tail(3).to_string(index=False))

# ============================================================
# 4. WRITE TO SNOWFLAKE
# ============================================================
conn = snowflake.connector.connect(**SNOWFLAKE_CONN)
cursor = conn.cursor()

cursor.execute("""
    CREATE TABLE IF NOT EXISTS VALUFLOW.RAW.SP500_PRICES (
        DATE    DATE,
        PRICE   FLOAT,
        VOLUME  BIGINT
    )
""")

cursor.execute("TRUNCATE TABLE VALUFLOW.RAW.SP500_PRICES")
print(f"\nTable truncated. Loading {len(df):,} rows...")

success, nchunks, nrows, _ = write_pandas(
    conn,
    df,
    table_name="SP500_PRICES",
    database="VALUFLOW",
    schema="RAW",
    overwrite=False,
    auto_create_table=False,
)

cursor.close()
conn.close()

print(f"Rows written to VALUFLOW.RAW.SP500_PRICES: {nrows:,}")
print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 60)