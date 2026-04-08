# ------- Regression Formula for Levered & Hamanda's Equation for Unlevered Beta ------- #
# -- using Snowflake's VALUFLOW.RAW.PRICE_DATA's PRICE Column for the SYMBOL "GSAT" -- #
# -- using FMP's S&P 500 Market_Returns -> stored in Snowflake's VALUFLOW.RAW.SP500_PRICES
# -- using Snowflake's Debt & Equity
# -- speeds FMP's import into Snowflake, efficiency improvement

#Imports
import snowflake.connector
import numpy as np
from dotenv import load_dotenv
import os
import requests

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

# Pull GSAT price data from Snowflake
cursor.execute("SELECT DATE, PRICE FROM PRICE_DATA WHERE TICKER = 'GSAT' ORDER BY DATE ASC")
rows = cursor.fetchall()

# Extract prices & calculate daily returns
prices = [row[1] for row in rows]
Stock_Returns = [(prices[i] - prices[i-1]) / prices[i-1] for i in range(1, len(prices))]

# Pull GSAT Total_Debt from Snowflake
cursor.execute("SELECT TOTALDEBT FROM BALANCE_SHEET WHERE TICKER = 'GSAT' ORDER BY DATE DESC LIMIT 1")
Debt = cursor.fetchone()[0]

# Pull GSAT Equity from Snowflake
cursor.execute("SELECT TOTALEQUITY FROM BALANCE_SHEET WHERE TICKER = 'GSAT' ORDER BY DATE DESC LIMIT 1")
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

# Pull S&P 500 prices from Snowflake & calculate Market_Returns
cursor.execute("SELECT PRICE FROM SP500_PRICES ORDER BY DATE ASC")
rows_market = cursor.fetchall()
prices_market = [row[0] for row in rows_market]
Market_Returns = [(prices_market[i] - prices_market[i-1]) / prices_market[i-1] for i in range(1, len(prices_market))]

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

print(f"Beta Levered = {beta_levered:.2f} | {levered_beta_volatility_message}")
print(f"Beta Unlevered = {beta_unlevered:.2f} | {unlevered_beta_volatility_message}")