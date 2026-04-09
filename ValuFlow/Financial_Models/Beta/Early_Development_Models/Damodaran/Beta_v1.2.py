# -- This folder serves as a working 'apply from reading' to Damodaran's Investment Valuation (4th Ed.) book
# -- Reason for reading and implementing book: after trying to test my Beta Model v1.3.1 & v1.4.0 to real values I realized the complexity and nuances that live inside professional grade Beta calculations. So I wanted to go back to the roots and really understand what is going on to be able to judge if my calculated Betas are reasonable (hopefully I will build a decisioning software that will be a sum of my analyses)

# -- Model Name: Beta_v2
# -- Model Changes
# -- Previous Model: Beta_v1
# -- Replaced fake data with real KO stock prices and S&P 500 data from Snowflake
# -- Replaced SQLAlchemy engine with snowflake.connector (password auth) to avoid MFA trigger
# -- Added date range lever (START_DATE, END_DATE) to control regression window
# -- Added frequency lever (FREQUENCY) with daily, weekly, monthly support

# -- Goal: Calculate a regression estimate of beta using KO's figures & use an alpha benchmark
# --       to see how well the stock did compared to expectations

# -- Regression Estimate of Beta formula:
# -- Rj = a + b * Rm

# -- Rj = return on the stock in a given period (same period as Rm)
# -- a  = alpha (intercept) — return when market is flat
# -- b  = beta (slope) — magnitude of stock movement relative to market
# -- Rm = return on the market in a given period (same period as Rj)

# -- DATA SOURCES --
# -- Stock Prices:   Snowflake -> VALUFLOW.RAW.PRICE_DATA
# -- Market Prices:  Snowflake -> VALUFLOW.RAW.SP500_PRICES

import os
import numpy as np
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
from datetime import date
import scipy.stats as stats

load_dotenv()

# ============================================================
# 0. USER INPUTS
# ============================================================
TICKER = "KO"
FREQUENCY = "monthly"       # options: "daily", "weekly", "monthly"
RISK_FREE_RATE = 0.0423


START_DATE = "2020-01-01"   # format: YYYY-MM-DD
END_DATE = "latest"         # options: "latest" or specific date YYYY-MM-DD

# ============================================================
# 1. SNOWFLAKE CONNECTION
# ============================================================
# -- Using snowflake.connector with password auth — avoids MFA trigger from SQLAlchemy
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database="VALUFLOW",
    schema="RAW"
)
cursor = conn.cursor()

# ============================================================
# 2. PULL PRICE DATA FROM SNOWFLAKE
# ============================================================
# -- Resolve END_DATE — "latest" uses today's date
end_date_resolved = date.today().strftime('%Y-%m-%d') if END_DATE == "latest" else END_DATE

# -- Pull ticker's stock prices within date range
cursor.execute(f"""
    SELECT DATE, PRICE 
    FROM VALUFLOW.RAW.PRICE_DATA 
    WHERE TICKER = '{TICKER}'
    AND DATE >= '{START_DATE}'
    AND DATE <= '{end_date_resolved}'
    ORDER BY DATE ASC
""")
rows_stock = cursor.fetchall()

# -- Pull S&P 500 prices within date range
cursor.execute(f"""
    SELECT DATE, PRICE 
    FROM VALUFLOW.RAW.SP500_PRICES 
    WHERE DATE >= '{START_DATE}'
    AND DATE <= '{end_date_resolved}'
    ORDER BY DATE ASC
""")
rows_market = cursor.fetchall()

print(f"Ticker:      {TICKER}")
print(f"Period:      {START_DATE} to {end_date_resolved}")
print(f"Stock rows:  {len(rows_stock)}")
print(f"Market rows: {len(rows_market)}")

# ============================================================
# 3. ALIGN DATES & CALCULATE RETURNS
# ============================================================
# -- Build dictionaries keyed by date for fast alignment
stock_dict = {row[0]: row[1] for row in rows_stock}
market_dict = {row[0]: row[1] for row in rows_market}

# -- Only use dates where both stock and market prices exist
# -- Prevents mismatched return periods from corrupting the regression
common_dates = sorted(set(stock_dict.keys()) & set(market_dict.keys()))

# -- Build aligned price series indexed by date
prices_stock = pd.Series(
    [stock_dict[d] for d in common_dates],
    index=pd.DatetimeIndex(common_dates)
)
prices_market = pd.Series(
    [market_dict[d] for d in common_dates],
    index=pd.DatetimeIndex(common_dates)
)

# -- Resample prices to selected frequency before calculating returns
if FREQUENCY == "weekly":
    prices_stock = prices_stock.resample("W").last()
    prices_market = prices_market.resample("W").last()
elif FREQUENCY == "monthly":
    prices_stock = prices_stock.resample("ME").last()
    prices_market = prices_market.resample("ME").last()

# -- Calculate period-over-period % returns after resampling
Stock_Returns = prices_stock.pct_change().dropna().tolist()
Market_Returns = prices_market.pct_change().dropna().tolist()

print(f"Common dates used:    {len(common_dates)}")
print(f"Return observations:  {len(Stock_Returns)}")

# ============================================================
# 3.1 FREQUENCY TRANSLATION
# ============================================================
# -- Converts frequency label to number of periods per year
# -- Used to scale the annual risk free rate to match return frequency
if FREQUENCY == 'daily':
    FREQUENCY_SUPPORT = 252
elif FREQUENCY == 'weekly':
    FREQUENCY_SUPPORT = 52
else:
    FREQUENCY_SUPPORT = 12

# ============================================================
# 4. COVARIANCE & VARIANCE
# ============================================================
# -- Covariance measures how stock and market returns move together
# -- Variance measures how much the market return moves on its own
cov = np.cov(Stock_Returns, Market_Returns)[0][1]
var = np.var(Market_Returns, ddof=1)

# ============================================================
# 5. BETA
# ============================================================
# -- Beta = Cov(Rj, Rm) / Var(Rm)
# -- Slope of the regression line — magnitude of stock movement relative to market
beta = cov / var

# ============================================================
# 6. R-SQUARED
# ============================================================
# -- R-squared = how much of the stock's movement is explained by the market
# -- (1 - R-squared) = firm-specific risk — movement unexplained by the market
correlation = np.corrcoef(Stock_Returns, Market_Returns)[0][1]
r_squared = correlation ** 2

# ============================================================
# 7. ALPHA (INTERCEPT)
# ============================================================
# -- Alpha = expected stock return when the market return is zero
# -- Compared against benchmark to assess whether stock over/underperformed expectations
mean_stock = np.mean(Stock_Returns)
mean_market = np.mean(Market_Returns)
alpha = mean_stock - beta * mean_market

# ============================================================
# 8. RISK FREE RATE
# ============================================================
# -- Hardcoded — 10-year US Treasury yield as of April 2026
# -- Replace with live FMP API call in future version
# -- Divided by FREQUENCY_SUPPORT to match the return period
risk_free_rate = RISK_FREE_RATE / FREQUENCY_SUPPORT

# ============================================================
# 9. ALPHA BENCHMARK
# ============================================================
# -- Benchmark = Rf * (1 - Beta)
# -- If alpha > benchmark -> stock outperformed expectations given its beta
# -- If alpha < benchmark -> stock underperformed expectations
# -- If alpha = benchmark -> stock performed exactly as expected
alpha_benchmark = risk_free_rate * (1 - beta)

# ============================================================
# 10. OUTPUT
# ============================================================
print(f"\n--- Regression Results ({FREQUENCY.capitalize()} | {TICKER} | {START_DATE} to {end_date_resolved}) ---")
print(f"Beta:              {beta:.6f}")
print(f"Alpha:             {alpha:.6f}")
print(f"R-squared:         {r_squared:.4f}")
print(f"1 - R-squared:     {1 - r_squared:.4f}  <- firm-specific risk")
print(f"Risk-Free Rate:    {risk_free_rate:.6f}  <- {FREQUENCY} equivalent")
print(f"Alpha Benchmark:   {alpha_benchmark:.6f}")

slope, intercept, r_value, p_value, std_err = stats.linregress(Market_Returns, Stock_Returns)
print(f"Beta:            {slope:.6f}")
print(f"Standard Error:  {std_err:.6f}")
print(f"P-value:         {p_value:.6f}")

if alpha > alpha_benchmark:
    print("\nAlpha performed better than expected.")
elif alpha < alpha_benchmark:
    print("\nAlpha performed worse than expected.")
else:
    print("\nAlpha performed as expected.")

# ============================================================
# 11. CLOSE CONNECTION
# ============================================================
cursor.close()
conn.close()