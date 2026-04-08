# ------- Hamada's Equation - Market Value of Debt (Dmv) Estimation ------- #
# ------- Model v1.3.3 ------- #

# -- PURPOSE --
# -- Estimates the Market Value of Debt (Dmv) for use in Hamada's Equation
# -- Dmv is approximated using the Damodaran Synthetic Rating Approach
# -- This script covers Step 1: Calculating the Pre-Tax and After-Tax Cost of Debt (Kd)

# -- METHODOLOGY --
# -- Step 1: Pull EBIT and Interest Expense from Snowflake (VALUFLOW.STAGING.FINANCIALS_CLEAN)
# -- Step 2: Calculate ICR (EBIT / Interest Expense) — Latest Year or 3-Year Average
# -- Step 3: Map ICR to Synthetic Credit Rating via Damodaran's live spread table (NYU's Website)
# -- Step 4: Kd = Risk-Free Rate (10-Year Treasury, live via FMP) + Default Spread
# -- Step 5: Apply Tax Shield -> Kd (After-Tax) = Kd (Pre-Tax) * (1 - Tax Rate)
# -- Next:   Use Kd to discount GSAT's debt cash flows -> Dmv

# -- DATA SOURCES --
# -- Fundamentals:    Snowflake -> VALUFLOW.STAGING.FINANCIALS_CLEAN
# -- Spread Table:    Damodaran (NYU) -> updates annually every January
# -- Risk-Free Rate:  FMP Treasury Rates API -> daily

# -- INPUTS --
# -- Ticker:          User defined at runtime
# -- ICR Method:      User defined at runtime (Latest Year or 3-Year Average)

# -- KNOWN LIMITATIONS --
# -- ICR-based Dmv is an estimation, not a directly observed market price
# -- GSAT has no publicly traded bond price accessible via free APIs (FINRA TRACE paywalled)
# -- Qualitative factors (Apple contract, Amazon acquisition talks, spectrum value) not captured
# -- Tax rate is 0% for GSAT due to NOL carryforwards — will dynamically update as earnings improve

# Imports
import pandas as pd
import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
import os
from datetime import date

load_dotenv()

# ============================================================
# 0. USER INPUTS
# ============================================================
TICKER = input("Enter Ticker: ").strip().upper()

print("\nICR Methodology:")
print("  1 - Latest Year Only")
print("  2 - 3-Year Average")
icr_choice = input("Select (1 or 2): ").strip()

while icr_choice not in ['1', '2']:
    print("Invalid selection. Please enter 1 or 2.")
    icr_choice = input("Select (1 or 2): ").strip()

ICR_METHOD = "latest" if icr_choice == '1' else "average"
print(f"\nRunning model for: {TICKER} | ICR Method: {'Latest Year' if ICR_METHOD == 'latest' else '3-Year Average'}")

# ============================================================
# 1. SNOWFLAKE CONNECTION
# ============================================================
engine = create_engine(URL(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database="VALUFLOW",
    schema="STAGING"
))

# ============================================================
# 2. PULL ICR INPUTS FROM SNOWFLAKE
# ============================================================
query = f"""
    SELECT
        TICKER,
        FISCALYEAR,
        EBIT,
        INTERESTEXPENSE
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

df = pd.read_sql(query, engine)
df.columns = df.columns.str.upper()

# ============================================================
# 3. CALCULATE ICR
# ============================================================
df['ICR'] = df['EBIT'] / df['INTERESTEXPENSE']

if ICR_METHOD == 'latest':
    avg_icr = df.iloc[-1]['ICR']
    method_label = f"Latest Year ({int(df.iloc[-1]['FISCALYEAR'])})"
else:
    avg_icr = df['ICR'].mean()
    method_label = "3-Year Average"

print(f"\n--- ICR Calculation ---")
print(df[['TICKER', 'FISCALYEAR', 'EBIT', 'INTERESTEXPENSE', 'ICR']])
print(f"\nICR Method:       {method_label}")
print(f"ICR Used:         {avg_icr:.4f}")

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

    # Large non-financial firms table — first 4 columns
    df_raw = tables[0].iloc[:, [0, 1, 2, 3]].copy()
    df_raw.columns = ['ICR_LOW', 'ICR_HIGH', 'RATING', 'SPREAD']

    # Drop rows where SPREAD contains non-numeric junk (headers, labels)
    df_raw = df_raw[df_raw['SPREAD'].astype(str).str.contains('%', na=False)]

    # Clean types
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
print(f"\nTimestamp: {date.today()} | Ticker: {TICKER} | ICR Method: {method_label} | ICR: {avg_icr:.4f} | Rating: {rating}")