# ------- Regression Formula for Levered & Hamanda's Equation for Unlevered Beta ------- #
# ------- Uses Book Value ------- #

# -- using Snowflake's VALUFLOW.RAW.PRICE_DATA's PRICE Column for the SYMBOL "VSAT" -- #
# -- using FMP's S&P 500 Market_Returns from FMP directly
# -- still using fake Debt & Equity numbers

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

# Pull VSAT price data from Snowflake
cursor = conn.cursor()
cursor.execute("SELECT DATE, PRICE FROM PRICE_DATA WHERE TICKER = 'VSAT' ORDER BY DATE ASC")
rows = cursor.fetchall()

# Extract prices & calculate daily returns
prices = [row[1] for row in rows]
Stock_Returns = [(prices[i] - prices[i-1]) / prices[i-1] for i in range(1, len(prices))]

cursor.close()
conn.close()

# Pull S&P 500 Market Returns from FMP
FMP_API_KEY = os.getenv("FMP_API_KEY")
url = f"https://financialmodelingprep.com/stable/historical-price-eod/light?symbol=%5EGSPC&from=2020-01-02&to=2026-04-07&apikey={FMP_API_KEY}"
response = requests.get(url)
data = response.json() 

prices_market = [day["price"] for day in reversed(data)]
Market_Returns = [(prices_market[i] - prices_market[i-1]) / prices_market[i-1] for i in range(1, len(prices_market))]

#Covariance of Stock & Market Returns
Cov = np.cov(Stock_Returns, Market_Returns)[0][1]

#Variance of Stock & Market Returns
Var = np.var(Market_Returns)

#Levered Beta Formula
beta_levered = Cov / Var

#Inputs
Debt = 500000
Equity = 1000000

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