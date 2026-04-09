# ------- Hamada's Equation - Full Market Value Beta Calculation ------- #
# ------- Model v1.4.0 ------- #

# -- CHANGES FROM PREVIOUS MODEL --
# -- Previous Model = v1.3.7
# -- Integrated regression logic from v1.3.1 to calculate levered beta (βL)
# -- Replaced book value Hamada unlevering with market value approach
# --   Old: βU = βL * (Equity / (Debt + Equity))          <- book value
# --   New: βU = βL / (1 + (1 - t) * (Dmv / Emv))        <- market value
# -- Dmv and Emv now feed directly into Hamada's Equation
# -- S&P 500 data pulled live from FMP and loaded into Snowflake each run
# -- βL and βU not written to Snowflake yet — printed to console only

# -- PURPOSE --
# -- Calculates levered beta (βL) via OLS regression of stock vs market returns
# -- Unlevels βL using market value D/E ratio via Hamada's Equation -> βU
# -- Dmv approximated using Damodaran Synthetic Rating Approach
# -- Emv pulled directly from Snowflake (market cap)
# -- Full market value capital structure used throughout — no book value inputs

# -- METHODOLOGY --
# -- Step 1:  Pull ICR, Implied Coupon Rate, and Total Debt from Snowflake (VALUFLOW.STAGING.VALUATION_INPUTS)
# -- Step 2:  Apply ICR methodology (Latest Year or 3-Year Average)
# -- Step 3:  Map ICR to Synthetic Credit Rating via Damodaran's live spread table (NYU's Website)
# -- Step 4:  Kd = Risk-Free Rate (10-Year Treasury, live via FMP) + Default Spread
# -- Step 5:  Apply Tax Shield -> Kd (After-Tax) = Kd (Pre-Tax) * (1 - Tax Rate)
# -- Step 6:  Discount semi-annual debt cash flows at Kd_pretax -> Dmv
# -- Step 7:  Write Dmv output to VALUFLOW.MODELS.DMV_OUTPUT with timestamp
# -- Step 8:  Pull Emv (market cap) from VALUFLOW.RAW.SHARES_OUTSTANDING
# -- Step 9:  Write Emv output to VALUFLOW.MODELS.EMV_OUTPUT with timestamp
# -- Step 10: Pull stock and S&P 500 price data -> calculate returns -> OLS regression -> βL
# -- Step 11: Unlever βL using market value D/E ratio via Hamada's Equation -> βU
# -- Next:    Use βU in CAPM to calculate cost of equity (Ke)

# -- DATA SOURCES --
# -- Valuation Inputs:    Snowflake -> VALUFLOW.STAGING.VALUATION_INPUTS
# -- Tax Rate:            Snowflake -> VALUFLOW.STAGING.FINANCIALS_CLEAN
# -- Shares Outstanding:  Snowflake -> VALUFLOW.RAW.SHARES_OUTSTANDING
# -- Price Data:          Snowflake -> VALUFLOW.RAW.PRICE_DATA
# -- S&P 500:             FMP API -> loaded into VALUFLOW.RAW.SP500_PRICES each run
# -- Spread Table:        Damodaran (NYU) -> updates annually every January
# -- Risk-Free Rate:      FMP Treasury Rates API -> daily

# -- INPUTS --
# -- Ticker:              Defined in Section 0
# -- Frequency:           Defined in Section 0 (daily, weekly, monthly)
# -- ICR Method:          Defined in Section 0 (latest or average of past 3 years)
# -- Maturity Years:      Defined in Section 0 (hardcoded from 10-K, updated manually per company)

# -- SNOWFLAKE CONNECTIONS --
# -- snowflake.connector (password auth) used for regression data pulls and SP500 write
# -- SQLAlchemy engine (password auth) used for pd.read_sql() queries
# -- snowflake.connector (private key auth) used for write_pandas() uploads to MODELS schema

# -- KNOWN LIMITATIONS --
# -- ICR-based Dmv is an estimation, not a directly observed market price
# -- Direct bond pricing not accessible via free APIs (FINRA TRACE paywalled, FMP has no bond data)
# -- Qualitative factors (partnerships, M&A activity, asset optionality) not captured in ICR
# -- Implied coupon rate is derived (Interest Expense / Total Debt), not sourced from debt schedule
# -- Maturity hardcoded from 10-K — requires manual update per company
# -- Emv derived as market cap — point-in-time from equity_data.py run
# -- Dmv and Emv written to Snowflake with timestamps — treat as point-in-time snapshots
# -- βL and βU not written to Snowflake yet — console output only
# -- Tax rate falls back to 0% for companies with NOL carryforwards — updates dynamically as earnings improve
# -- S&P 500 data pulled fresh each run and overwrites VALUFLOW.RAW.SP500_PRICES

# Imports
import pandas as pd
import numpy as np
import requests
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from cryptography.hazmat.primitives import serialization
from dotenv import load_dotenv
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
import os
from datetime import date, datetime

load_dotenv()

# ============================================================
# 0. USER INPUTS
# ============================================================
TICKER = "KO"
FREQUENCY = "monthly"       # options: "daily", "weekly", "monthly"
ICR_METHOD = "average"      # options: "latest" or "average" (average uses last 3 years)
MATURITY_YEARS = 3          # years to maturity — sourced from GSAT 10-K (Senior Notes due 2029)

# ============================================================
# 1. SNOWFLAKE CONNECTIONS
# ============================================================
# SQLAlchemy engine for pd.read_sql() reads
engine = create_engine(URL(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database="VALUFLOW",
    schema="STAGING"
))

# snowflake.connector for regression data pulls and SP500 write
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
# 2. PULL ICR, IMPLIED COUPON RATE, AND TOTAL DEBT FROM SNOWFLAKE
# ============================================================
query = f"""
    SELECT
        TICKER,
        FISCALYEAR,
        INTEREST_COVERAGE,
        IMPLIED_COUPON_RATE,
        TOTAL_DEBT
    FROM VALUFLOW.STAGING.VALUATION_INPUTS
    WHERE TICKER = '{TICKER}'
    AND FISCALYEAR IN (
        SELECT DISTINCT FISCALYEAR
        FROM VALUFLOW.STAGING.VALUATION_INPUTS
        WHERE TICKER = '{TICKER}'
        ORDER BY FISCALYEAR DESC
        LIMIT 3
    )
    ORDER BY FISCALYEAR ASC
"""

df = pd.read_sql(query, engine)
df.columns = df.columns.str.upper()

# ============================================================
# 3. APPLY ICR METHODOLOGY
# ============================================================
if ICR_METHOD == 'latest':
    avg_icr = df.iloc[-1]['INTEREST_COVERAGE']
    implied_coupon = df.iloc[-1]['IMPLIED_COUPON_RATE']
    total_debt = df.iloc[-1]['TOTAL_DEBT']
    method_label = f"Latest Year ({int(df.iloc[-1]['FISCALYEAR'])})"
else:
    avg_icr = df['INTEREST_COVERAGE'].mean()
    implied_coupon = df.iloc[-1]['IMPLIED_COUPON_RATE']  # always use most recent coupon
    total_debt = df.iloc[-1]['TOTAL_DEBT']               # always use most recent debt balance
    method_label = "3-Year Average"

print(f"\n--- ICR & Debt Inputs ---")
print(df[['TICKER', 'FISCALYEAR', 'INTEREST_COVERAGE', 'IMPLIED_COUPON_RATE', 'TOTAL_DEBT']])
print(f"\nICR Method:           {method_label}")
print(f"ICR Used:             {avg_icr:.4f}")
print(f"Implied Coupon Rate:  {implied_coupon:.2%}")
print(f"Total Debt:           ${total_debt:,.0f}")

# ============================================================
# 3.5 PULL TAX RATE FROM SNOWFLAKE
# ============================================================
tax_query = f"""
    SELECT
        TICKER,
        FISCALYEAR,
        TAX_RATE
    FROM VALUFLOW.STAGING.FINANCIALS_CLEAN
    WHERE TICKER = '{TICKER}'
    AND FISCALYEAR IN (
        SELECT DISTINCT FISCALYEAR
        FROM VALUFLOW.STAGING.FINANCIALS_CLEAN
        WHERE TICKER = '{TICKER}'
        ORDER BY FISCALYEAR DESC
        LIMIT 3
    )
    ORDER BY FISCALYEAR ASC
"""

df_tax = pd.read_sql(tax_query, engine)
df_tax.columns = df_tax.columns.str.upper()

# Use most recent non-zero tax rate, fall back to 0 if all zero
non_zero_tax = df_tax[df_tax['TAX_RATE'] > 0]['TAX_RATE']
tax_rate = non_zero_tax.iloc[-1] if not non_zero_tax.empty else 0.0

print("\n--- Tax Rate ---")
print(df_tax[['TICKER', 'FISCALYEAR', 'TAX_RATE']])
print(f"\nTax Rate Used: {tax_rate:.2%}")

# ============================================================
# 4. SCRAPE DAMODARAN SPREAD TABLE (LIVE - UPDATES ANNUALLY)
# ============================================================
def get_damodaran_spreads():
    url = "https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ratings.html"
    tables = pd.read_html(url)

    df_raw = tables[0].iloc[:, [0, 1, 2, 3]].copy()
    df_raw.columns = ['ICR_LOW', 'ICR_HIGH', 'RATING', 'SPREAD']
    df_raw = df_raw[df_raw['SPREAD'].astype(str).str.contains('%', na=False)]

    df_raw['ICR_LOW'] = pd.to_numeric(df_raw['ICR_LOW'], errors='coerce')
    df_raw['ICR_HIGH'] = pd.to_numeric(df_raw['ICR_HIGH'], errors='coerce')
    df_raw['SPREAD'] = (
        df_raw['SPREAD']
        .astype(str)
        .str.replace('%', '', regex=False)
        .str.strip()
        .astype(float) / 100
    )

    return df_raw.dropna().reset_index(drop=True)

def map_icr_to_spread(avg_icr, spread_table):
    match = spread_table[
        (spread_table['ICR_LOW'] <= avg_icr) &
        (spread_table['ICR_HIGH'] > avg_icr)
    ]
    if match.empty:
        return None, None
    return match['RATING'].values[0], match['SPREAD'].values[0]

spread_table = get_damodaran_spreads()
rating, default_spread = map_icr_to_spread(avg_icr, spread_table)

print("\n--- Damodaran Synthetic Rating ---")
print(f"Synthetic Rating:  {rating}")
print(f"Default Spread:    {default_spread:.2%}")

# ============================================================
# 5. PULL LIVE 10-YEAR TREASURY RATE FROM FMP
# ============================================================
def get_risk_free_rate():
    url = "https://financialmodelingprep.com/stable/treasury-rates"
    params = {"apikey": os.getenv("FMP_API_KEY")}
    response = requests.get(url, params=params)
    data = response.json()

    latest = data[0]
    rfr = latest['year10'] / 100
    pulled_date = latest['date']
    print(f"\n--- Risk-Free Rate ---")
    print(f"10-Year Treasury Rate: {rfr:.2%} (as of {pulled_date})")
    return rfr

risk_free_rate = get_risk_free_rate()

# ============================================================
# 6. CALCULATE Kd (PRE-TAX AND AFTER-TAX COST OF DEBT)
# ============================================================
Kd_pretax = risk_free_rate + default_spread
Kd_aftertax = Kd_pretax * (1 - tax_rate)

print("\n--- Cost of Debt (Kd) ---")
print(f"Risk-Free Rate:      {risk_free_rate:.2%}")
print(f"Default Spread:      {default_spread:.2%}")
print(f"Kd (Pre-Tax):        {Kd_pretax:.2%}")
print(f"Tax Rate:            {tax_rate:.2%}")
print(f"Kd (After-Tax):      {Kd_aftertax:.2%}")

# ============================================================
# 7. CALCULATE Dmv (MARKET VALUE OF DEBT — SEMI-ANNUAL)
# ============================================================
periods = MATURITY_YEARS * 2
semi_annual_coupon = (implied_coupon / 2) * total_debt
semi_annual_kd = Kd_pretax / 2

pv_coupons = sum(
    semi_annual_coupon / (1 + semi_annual_kd) ** t
    for t in range(1, periods + 1)
)
pv_principal = total_debt / (1 + semi_annual_kd) ** periods
Dmv = pv_coupons + pv_principal

print("\n--- Market Value of Debt (Dmv) ---")
print(f"Total Debt (Face Value):    ${total_debt:,.0f}")
print(f"Implied Coupon Rate:        {implied_coupon:.2%}")
print(f"Maturity:                   {MATURITY_YEARS} years ({periods} semi-annual periods)")
print(f"Semi-Annual Coupon:         ${semi_annual_coupon:,.0f}")
print(f"Semi-Annual Kd:             {semi_annual_kd:.2%}")
print(f"PV of Coupons:              ${pv_coupons:,.0f}")
print(f"PV of Principal:            ${pv_principal:,.0f}")
print(f"Dmv (Market Value of Debt): ${Dmv:,.0f}")

# ============================================================
# 8. WRITE Dmv TO SNOWFLAKE WITH TIMESTAMP
# ============================================================
private_key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
with open(private_key_path, "rb") as key_file:
    private_key = serialization.load_pem_private_key(key_file.read(), password=None)

private_key_bytes = private_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption()
)

conn_models = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database="VALUFLOW",
    private_key=private_key_bytes,
    role="SYSADMIN"
)

df_dmv = pd.DataFrame([{
    "TICKER":               TICKER,
    "CALCULATION_DATE":     datetime.today().strftime('%Y-%m-%d'),
    "ICR_METHOD":           method_label,
    "ICR_USED":             round(avg_icr, 6),
    "SYNTHETIC_RATING":     rating,
    "DEFAULT_SPREAD":       round(default_spread, 6),
    "RISK_FREE_RATE":       round(risk_free_rate, 6),
    "KD_PRETAX":            round(Kd_pretax, 6),
    "TAX_RATE":             round(tax_rate, 6),
    "KD_AFTERTAX":          round(Kd_aftertax, 6),
    "IMPLIED_COUPON_RATE":  round(implied_coupon, 6),
    "TOTAL_DEBT_FACE":      round(total_debt, 2),
    "MATURITY_YEARS":       MATURITY_YEARS,
    "PV_COUPONS":           round(pv_coupons, 2),
    "PV_PRINCIPAL":         round(pv_principal, 2),
    "DMV":                  round(Dmv, 2)
}])

conn_models.cursor().execute("CREATE SCHEMA IF NOT EXISTS MODELS")
conn_models.cursor().execute("USE SCHEMA MODELS")

write_pandas(
    conn_models, df_dmv, "DMV_OUTPUT",
    schema="MODELS",
    auto_create_table=True,
    overwrite=False
)
print(f"\nDmv written to VALUFLOW.MODELS.DMV_OUTPUT")

# ============================================================
# 9. PULL Emv FROM SNOWFLAKE
# ============================================================
emv_query = f"""
    SELECT
        TICKER,
        MARKET_CAP,
        PRICE,
        SHARES_OUTSTANDING,
        PULLED_DATE
    FROM VALUFLOW.RAW.SHARES_OUTSTANDING
    WHERE TICKER = '{TICKER}'
"""

df_emv = pd.read_sql(emv_query, engine)
df_emv.columns = df_emv.columns.str.upper()

Emv = df_emv.iloc[0]['MARKET_CAP']
emv_price = df_emv.iloc[0]['PRICE']
emv_shares = df_emv.iloc[0]['SHARES_OUTSTANDING']
emv_pulled_date = df_emv.iloc[0]['PULLED_DATE']

print("\n--- Market Value of Equity (Emv) ---")
print(f"Share Price:                  ${emv_price:,.2f}")
print(f"Shares Outstanding:           {emv_shares:,.0f}")
print(f"Emv (Market Cap):             ${Emv:,.0f}")
print(f"Equity Data As Of:            {emv_pulled_date}")

# ============================================================
# 10. WRITE Emv TO SNOWFLAKE WITH TIMESTAMP
# ============================================================
df_emv_output = pd.DataFrame([{
    "TICKER":               TICKER,
    "CALCULATION_DATE":     datetime.today().strftime('%Y-%m-%d'),
    "SHARE_PRICE":          round(emv_price, 2),
    "SHARES_OUTSTANDING":   round(emv_shares, 0),
    "EMV":                  round(Emv, 2),
    "EQUITY_DATA_AS_OF":    str(emv_pulled_date)
}])

write_pandas(
    conn_models, df_emv_output, "EMV_OUTPUT",
    schema="MODELS",
    auto_create_table=True,
    overwrite=False
)
print(f"Emv written to VALUFLOW.MODELS.EMV_OUTPUT")

# ============================================================
# 11. PULL PRICE DATA AND CALCULATE LEVERED BETA (βL)
# ============================================================
print(f"\n--- Beta Regression ({FREQUENCY.capitalize()}) ---")

# Pull and refresh S&P 500 data from FMP
FMP_API_KEY = os.getenv("FMP_API_KEY")
url = f"https://financialmodelingprep.com/stable/historical-price-eod/light?symbol=%5EGSPC&from=2020-01-02&to={date.today()}&apikey={FMP_API_KEY}"
response = requests.get(url)
sp500_data = response.json()

cursor.execute("""
    CREATE TABLE IF NOT EXISTS SP500_PRICES (
        DATE DATE,
        PRICE FLOAT,
        VOLUME BIGINT
    )
""")
cursor.execute("TRUNCATE TABLE SP500_PRICES")
rows_to_insert = [(day["date"], day["price"], day["volume"]) for day in sp500_data]
cursor.executemany("""
    INSERT INTO SP500_PRICES (DATE, PRICE, VOLUME)
    VALUES (%s, %s, %s)
""", rows_to_insert)
conn.commit()

# Pull stock price data
cursor.execute(f"SELECT DATE, PRICE FROM PRICE_DATA WHERE TICKER = '{TICKER}' ORDER BY DATE ASC")
rows_stock = cursor.fetchall()

# Pull S&P 500 prices
cursor.execute("SELECT DATE, PRICE FROM SP500_PRICES ORDER BY DATE ASC")
rows_market = cursor.fetchall()

# Build aligned price series
stock_dict = {datetime.strptime(row[0], "%Y-%m-%d").date(): row[1] for row in rows_stock}
market_dict = {row[0]: row[1] for row in rows_market}
common_dates = sorted(set(stock_dict.keys()) & set(market_dict.keys()))

prices_stock = pd.Series(
    [stock_dict[d] for d in common_dates],
    index=pd.DatetimeIndex(common_dates)
)
prices_market = pd.Series(
    [market_dict[d] for d in common_dates],
    index=pd.DatetimeIndex(common_dates)
)

# Resample based on frequency
if FREQUENCY == "weekly":
    prices_stock = prices_stock.resample("W").last()
    prices_market = prices_market.resample("W").last()
elif FREQUENCY == "monthly":
    prices_stock = prices_stock.resample("ME").last()
    prices_market = prices_market.resample("ME").last()

# Calculate returns
stock_returns = prices_stock.pct_change().dropna().tolist()
market_returns = prices_market.pct_change().dropna().tolist()

# OLS regression -> βL
cov = np.cov(stock_returns, market_returns)[0][1]
var = np.var(market_returns)
beta_levered = cov / var

print(f"Stock Returns:   {len(stock_returns)} {FREQUENCY} observations")
print(f"Market Returns:  {len(market_returns)} {FREQUENCY} observations")
print(f"βL (Levered Beta): {beta_levered:.4f}")

# ============================================================
# 12. HAMADA'S EQUATION — UNLEVER BETA USING MARKET VALUE D/E
# ============================================================
DE_ratio = Dmv / Emv
beta_unlevered = beta_levered / (1 + (1 - tax_rate) * DE_ratio)

if beta_levered > 1:
    levered_msg = f"Stock is {((beta_levered - 1) * 100):.0f}% more volatile than the market"
elif beta_levered < 1:
    levered_msg = f"Stock is {((1 - beta_levered) * 100):.0f}% less volatile than the market"
else:
    levered_msg = "Stock moves in line with the market"

if beta_unlevered > 1:
    unlevered_msg = f"Business is {((beta_unlevered - 1) * 100):.0f}% more volatile than the market"
elif beta_unlevered < 1:
    unlevered_msg = f"Business is {((1 - beta_unlevered) * 100):.0f}% less volatile than the market"
else:
    unlevered_msg = "Business moves in line with the market"

print(f"\n--- Hamada's Equation (Market Value) ---")
print(f"Dmv:                      ${Dmv:,.0f}")
print(f"Emv:                      ${Emv:,.0f}")
print(f"D/E Ratio (Market Value): {DE_ratio:.4f}")
print(f"Tax Rate:                 {tax_rate:.2%}")
print(f"βL (Levered Beta):        {beta_levered:.4f} | {levered_msg}")
print(f"βU (Unlevered Beta):      {beta_unlevered:.4f} | {unlevered_msg}")

# Close connections
cursor.close()
conn.close()
conn_models.close()

print(f"\nTimestamp: {date.today()} | Ticker: {TICKER} | Frequency: {FREQUENCY} | ICR Method: {method_label}")
print(f"Rating: {rating} | Dmv: ${Dmv:,.0f} | Emv: ${Emv:,.0f} | D/E: {DE_ratio:.4f} | βL: {beta_levered:.4f} | βU: {beta_unlevered:.4f}")