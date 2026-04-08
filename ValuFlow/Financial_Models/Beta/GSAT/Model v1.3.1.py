# ------- Regression Formula for Levered & Hamanda's Equation for Unlevered Beta ------- #
# -- using Snowflake's VALUFLOW.RAW.PRICE_DATA's PRICE Column for the input TICKER -- #
# -- using FMP's S&P 500 Market_Returns -> stored in Snowflake's VALUFLOW.RAW.SP500_PRICES
# -- using Snowflake's Debt & Equity
# -- speeds FMP's import into Snowflake, efficiency improvement

# -- allows for a simple ticker input
# -- allows for a simple ticker frequency input
# -- trying to fix GSAT personal calculated Beta vs Beta found on the web -- deemed fixed (needed to use monthly return frequency), online Betas are within a 7% difference

#Imports
import snowflake.connector
import numpy as np
from dotenv import load_dotenv
import os
import requests
from datetime import datetime
import pandas as pd

#---------- INPUT ----------#
TICKER = "GSAT"         #  only works for companies I already have in Snowflake DB
FREQUENCY = "monthly"         # options: "daily", "weekly", "monthly"
#---------------------------#

#Load Snowflake credentials from .env file
load_dotenv()

# Connect to Snowflake
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database="VALUFLOW",
    schema="RAW"
)

cursor = conn.cursor()

# Pull Total_Debt from Snowflake
cursor.execute(f"SELECT TOTALDEBT FROM BALANCE_SHEET WHERE TICKER = '{TICKER}' ORDER BY DATE DESC LIMIT 1")
Debt = cursor.fetchone()[0]

# Pull Equity from Snowflake
cursor.execute(f"SELECT TOTALEQUITY FROM BALANCE_SHEET WHERE TICKER = '{TICKER}' ORDER BY DATE DESC LIMIT 1")
Equity = cursor.fetchone()[0]

# Pull S&P 500 data from FMP
FMP_API_KEY = os.getenv("FMP_API_KEY")
url = f"https://financialmodelingprep.com/stable/historical-price-eod/light?symbol=%5EGSPC&from=2020-01-02&to=2026-04-07&apikey={FMP_API_KEY}"
response = requests.get(url)
data = response.json()

# Create SP500_PRICES table if it doesn't exist
cursor.execute("""
    CREATE TABLE IF NOT EXISTS SP500_PRICES (
        DATE DATE,
        PRICE FLOAT,
        VOLUME BIGINT
    )
""")

# Truncate and reload with fresh data
cursor.execute("TRUNCATE TABLE SP500_PRICES")
rows_to_insert = [(day["date"], day["price"], day["volume"]) for day in data]
cursor.executemany("""
    INSERT INTO SP500_PRICES (DATE, PRICE, VOLUME)
    VALUES (%s, %s, %s)
""", rows_to_insert)

conn.commit()

# Pull price data from Snowflake
cursor.execute(f"SELECT DATE, PRICE FROM PRICE_DATA WHERE TICKER = '{TICKER}' ORDER BY DATE ASC")
rows = cursor.fetchall()

# Pull S&P 500 prices from Snowflake
cursor.execute("SELECT DATE, PRICE FROM SP500_PRICES ORDER BY DATE ASC")
rows_market = cursor.fetchall()

from datetime import datetime
import pandas as pd

# ----------------------- Calculation Block ----------------------- #

# Convert to dictionaries keyed by date
stock_dict = {datetime.strptime(row[0], "%Y-%m-%d").date(): row[1] for row in rows}
market_dict = {row[0]: row[1] for row in rows_market}

# Find dates that exist in BOTH
common_dates = sorted(set(stock_dict.keys()) & set(market_dict.keys()))

# Build aligned price series
import pandas as pd
prices_stock_aligned = pd.Series(
    [stock_dict[date] for date in common_dates], 
    index=pd.DatetimeIndex(common_dates)
)
prices_market_aligned = pd.Series(
    [market_dict[date] for date in common_dates], 
    index=pd.DatetimeIndex(common_dates)
)

# Resample based on FREQUENCY
if FREQUENCY == "weekly":
    prices_stock_aligned = prices_stock_aligned.resample("W").last()
    prices_market_aligned = prices_market_aligned.resample("W").last()
elif FREQUENCY == "monthly":
    prices_stock_aligned = prices_stock_aligned.resample("ME").last()
    prices_market_aligned = prices_market_aligned.resample("ME").last()

# Calculate returns
Stock_Returns = prices_stock_aligned.pct_change().dropna().tolist()
Market_Returns = prices_market_aligned.pct_change().dropna().tolist()
cursor.close()
conn.close()

#Covariance of Stock & Market Returns
Cov = np.cov(Stock_Returns, Market_Returns)[0][1]

#Variance of Stock & Market Returns
Var = np.var(Market_Returns)

#Levered Beta Formula
beta_levered = Cov / Var

#Unlevered Beta
beta_unlevered = beta_levered * (Equity / (Debt + Equity))

# ----------------------- Calculation Block ----------------------- #

if beta_levered > 1:
    levered_beta_volatility_message = f"The stock is {((beta_levered - 1) * 100):.0f}% more volatile than the market"
elif beta_levered < 1:
    levered_beta_volatility_message = f"The stock is {((1 - beta_levered) * 100):.0f}% less volatile than the market"
else:
    levered_beta_volatility_message = "The stock moves in line with the market"

if beta_unlevered > 1:
    unlevered_beta_volatility_message = f"The business is {((beta_unlevered - 1) * 100):.0f}% more volatile than the market"
elif beta_unlevered < 1:
    unlevered_beta_volatility_message = f"The business is {((1 - beta_unlevered) * 100):.0f}% less volatile than the market"
else:
    unlevered_beta_volatility_message = "The business moves in line with the market"

print(f"Ticker: {TICKER}")
print(f"Beta Levered = {beta_levered:.2f} | {levered_beta_volatility_message}")
print(f"Beta Unlevered = {beta_unlevered:.2f} | {unlevered_beta_volatility_message}")