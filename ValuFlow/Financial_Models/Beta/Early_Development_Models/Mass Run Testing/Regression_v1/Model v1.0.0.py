# ------- ValuFlow - Beta Sensitivity Analysis ------- #
# ------- Mass Run - Regression Flow (Model v1.0.0).py ------- #

# -- ANALYSIS NAME: Beta Sensitivity Analysis: Regression Flow
# -- GOAL: For a given industry/sector and time window, calculate for each ticker:
# --   1. Regression Beta   — market beta from price return regression vs S&P 500
# --   2. Levered Beta      — same as regression beta (market-observed, includes leverage)
# --   3. Unlevered Beta    — Hamada equation strips out financial leverage to isolate business risk
# --   4. R-squared         — how much of stock movement is explained by the market
# --   5. Alpha             — intercept vs benchmark to assess over/underperformance
# --   6. Standard Error    — precision of the beta estimate
# --   7. P-value           — statistical significance of the beta

# -- HAMADA EQUATION (Book Value):
# --   Beta_Unlevered = Beta_Levered / (1 + (1 - Tax_Rate) * (Debt / Equity))
# --   Tax Rate: 21% (US corporate, fixed)
# --   Debt:   TOTAL_DEBT from STAGING.BALANCE_SHEET_ANNUAL (most recent annual filing)
# --   Equity: TOTAL_STOCKHOLDERS_EQUITY from STAGING.BALANCE_SHEET_ANNUAL (most recent annual filing)

# -- RISK-FREE RATE:
# --   Source: VALUFLOW.RAW.TREASURY_RATES (10-year, YEAR_10 column)
# --   Method: Average daily YEAR_10 rate over the regression window (START_DATE to END_DATE)
# --   Run treasury_ingest.py first to populate this table

# -- FREQUENCY SENSITIVITY:
# --   Set FREQUENCIES to any combination of "daily", "weekly", "monthly"
# --   Each frequency runs as a full pass — ticker x frequency = one row in output

# -- SECTOR:
# --   Set SECTOR = "All" to run every sector in TICKER_UNIVERSE
# --   Set SECTOR = "Healthcare" (or any valid sector name) to run a single sector

# -- NEW OUTPUT COLUMNS (v1.0.0):
# --   BETA_DISTORTION     — raw gap between levered and unlevered beta (Beta_L - Beta_U)
# --                         positive = leverage inflates perceived market risk above true business risk
# --   BETA_DISTORTION_PCT — percentage distortion relative to unlevered beta
# --                         normalizes the gap so industries with different beta magnitudes are comparable
# --   RUN_ID              — unique identifier for each execution (UUID), allows multiple same-day runs
# --                         to be distinguished in the output table without ambiguity
# --   RUN_TIMESTAMP       — full datetime of when the script was executed (not just date)

# -- KNOWN LIMITATIONS:
# --   1. Point-in-Time Mismatch (Balance Sheet vs Regression Window)
# --      The balance sheet query always pulls the most recent annual filing regardless of the
# --      regression date window. If START_DATE is 2020-01-01, a company's 2024 D/E ratio is
# --      being used to unlever a beta estimated partly from 2020 prices. The leverage structure
# --      during the regression period is not what is being used in Hamada — this is a known
# --      limitation to be addressed in a future version with time-matched balance sheet pulls.
# --
# --   2. Frequency-Agnostic MIN_OBSERVATIONS
# --      MIN_OBSERVATIONS = 12 is applied identically across daily, weekly, and monthly runs.
# --      12 monthly observations = 1 year of data (reasonable floor).
# --      12 daily observations   = ~2.5 weeks of data (effectively meaningless for beta estimation).
# --      A future version should apply frequency-aware minimums (daily=252, weekly=52, monthly=12).

# -- DATA SOURCES:
# --   Price data:       VALUFLOW.STAGING.PRICE_DATA
# --   S&P 500 prices:   VALUFLOW.RAW.SP500_PRICES
# --   Debt & Equity:    VALUFLOW.STAGING.BALANCE_SHEET_ANNUAL
# --   Ticker universe:  VALUFLOW.RAW.TICKER_UNIVERSE
# --   Risk-free rate:   VALUFLOW.RAW.TREASURY_RATES
# --   Output:           VFMODELS.BETA.BETA_SENSITIVITY_REGRESSION_FLOW_V1

import os
import uuid
import traceback
import numpy as np
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from cryptography.hazmat.primitives import serialization
from dotenv import load_dotenv
from datetime import date, datetime
import scipy.stats as stats

load_dotenv(dotenv_path=r"C:\Users\timel\Desktop\ValuFlow\.env", override=True)

# ============================================================
# 0. USER INPUTS
# ============================================================
SECTOR           = "All"                            # "All" or a specific sector name
INDUSTRY         = None                             # String to filter further, or None for all
FREQUENCIES      = ["daily", "weekly", "monthly"]  # Any combination of the three
START_DATE       = "2020-01-01"                     # format: YYYY-MM-DD
END_DATE         = "latest"                         # "latest" or specific date YYYY-MM-DD
TAX_RATE         = 0.21                             # US corporate tax rate (fixed)
MIN_OBSERVATIONS = 12                               # Minimum return observations required

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
    "user":            os.getenv("SNOWFLAKE_USER"),
    "account":         os.getenv("SNOWFLAKE_ACCOUNT"),
    "warehouse":       os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database":        "VALUFLOW",
    "private_key":     private_key_bytes,
    "role":            "SYSADMIN",
    "network_timeout": 300,
    "login_timeout":   60,
}

# ============================================================
# 2. RESOLVE DATE RANGE
# ============================================================
end_date_resolved = date.today().strftime('%Y-%m-%d') if END_DATE == "latest" else END_DATE

# ============================================================
# 3. RUN IDENTITY
# -- RUN_ID:        unique UUID per execution — distinguishes multiple same-day runs
# -- RUN_TIMESTAMP: full datetime of execution — more precise than RUN_DATE alone
# ============================================================
RUN_ID        = str(uuid.uuid4())
RUN_TIMESTAMP = datetime.now()

# ============================================================
# 4. FREQUENCY SETTINGS
# ============================================================
FREQ_MAP = {"daily": 252, "weekly": 52, "monthly": 12}

def build_return_dict(rows, date_col_idx, price_col_idx, frequency):
    """
    Takes raw Snowflake rows, resamples to period-end prices,
    computes returns, and returns a dict keyed by string period label.
    No pandas index gymnastics — uses explicit string keys throughout.
    """
    df = pd.DataFrame({"DATE": pd.to_datetime([r[date_col_idx] for r in rows]),
                        "PRICE": [float(r[price_col_idx]) for r in rows]})
    df = df.sort_values("DATE").reset_index(drop=True).set_index("DATE")

    if frequency == "monthly":
        df_resampled  = df["PRICE"].resample("ME").last().dropna()
        period_labels = [d.strftime("%Y-%m") for d in df_resampled.index]
    elif frequency == "weekly":
        df_resampled  = df["PRICE"].resample("W").last().dropna()
        period_labels = [d.strftime("%Y-W%W") for d in df_resampled.index]
    else:
        df_resampled  = df["PRICE"].copy()
        period_labels = [d.strftime("%Y-%m-%d") for d in df_resampled.index]

    prices_arr = df_resampled.values
    returns    = np.diff(prices_arr) / prices_arr[:-1]
    labels     = period_labels[1:]

    return dict(zip(labels, returns))

# ============================================================
# 5. SETUP — CREATE OUTPUT DATABASE/SCHEMA/TABLE
# ============================================================
print("=" * 65)
print("ValuFlow -- Beta Sensitivity Analysis (Regression Flow)")
print(f"Started:     {RUN_TIMESTAMP.strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Run ID:      {RUN_ID}")
print(f"Sector:      {SECTOR}  |  Industry: {INDUSTRY or 'All'}")
print(f"Period:      {START_DATE} to {end_date_resolved}")
print(f"Frequencies: {', '.join(FREQUENCIES)}")
print("=" * 65)

conn   = snowflake.connector.connect(**SNOWFLAKE_CONN)
cursor = conn.cursor()

cursor.execute("CREATE DATABASE IF NOT EXISTS VFMODELS")
cursor.execute("CREATE SCHEMA IF NOT EXISTS VFMODELS.BETA")
cursor.execute("""
    CREATE TABLE IF NOT EXISTS VFMODELS.BETA.BETA_SENSITIVITY_REGRESSION_FLOW_V1 (
        RUN_ID                  VARCHAR,
        RUN_TIMESTAMP           TIMESTAMP_NTZ,
        RUN_DATE                DATE,
        TICKER                  VARCHAR,
        COMPANY_NAME            VARCHAR,
        SECTOR                  VARCHAR,
        INDUSTRY                VARCHAR,
        FREQUENCY               VARCHAR,
        START_DATE              DATE,
        END_DATE                DATE,
        OBSERVATIONS            NUMBER,
        BETA_LEVERED            FLOAT,
        BETA_UNLEVERED          FLOAT,
        BETA_DISTORTION         FLOAT,
        BETA_DISTORTION_PCT     FLOAT,
        ALPHA                   FLOAT,
        ALPHA_BENCHMARK         FLOAT,
        ALPHA_VS_BENCHMARK      VARCHAR,
        R_SQUARED               FLOAT,
        FIRM_SPECIFIC_RISK      FLOAT,
        STD_ERROR               FLOAT,
        P_VALUE                 FLOAT,
        RISK_FREE_RATE_ANNUAL   FLOAT,
        TAX_RATE                FLOAT,
        TOTAL_DEBT              FLOAT,
        TOTAL_EQUITY            FLOAT,
        DEBT_TO_EQUITY          FLOAT,
        NOTES                   VARCHAR
    )
""")
print("VFMODELS.BETA schema and output table ready\n")

# ============================================================
# 6. PULL POINT-IN-TIME RISK-FREE RATE FROM TREASURY TABLE
# ============================================================
cursor.execute(f"""
    SELECT AVG(YEAR_10)
    FROM VALUFLOW.RAW.TREASURY_RATES
    WHERE RATE_DATE >= '{START_DATE}'
    AND RATE_DATE <= '{end_date_resolved}'
    AND YEAR_10 IS NOT NULL
""")
rf_result = cursor.fetchone()[0]

if rf_result is None:
    print("\nWARNING: No Treasury rate data found for this window.")
    print("Run treasury_ingest.py first, then retry.")
    cursor.close()
    conn.close()
    exit()

RISK_FREE_RATE = rf_result / 100 if rf_result > 1 else rf_result
print(f"Risk-Free Rate (10Y avg over window): {RISK_FREE_RATE:.4%}\n")

# ============================================================
# 7. PULL TICKER UNIVERSE FOR SECTOR/INDUSTRY
# ============================================================
if SECTOR == "All":
    sector_clause   = ""
    industry_filter = f"WHERE INDUSTRY = '{INDUSTRY}'" if INDUSTRY else ""
else:
    sector_clause   = f"WHERE SECTOR = '{SECTOR}'"
    industry_filter = f"AND INDUSTRY = '{INDUSTRY}'" if INDUSTRY else ""

cursor.execute(f"""
    SELECT TICKER, COMPANY_NAME, SECTOR, INDUSTRY
    FROM VALUFLOW.RAW.TICKER_UNIVERSE
    {sector_clause}
    {industry_filter}
    ORDER BY TICKER
""")
universe_rows = cursor.fetchall()

if not universe_rows:
    print(f"No tickers found for SECTOR='{SECTOR}' INDUSTRY='{INDUSTRY}'. Check TICKER_UNIVERSE values.")
    cursor.close()
    conn.close()
    exit()

print(f"Tickers in universe: {len(universe_rows)}")
print(f"Frequency passes:    {len(FREQUENCIES)}")
print(f"Total runs:          {len(universe_rows) * len(FREQUENCIES)}\n")

# ============================================================
# 8. PULL S&P 500 PRICES ONCE — USED ACROSS ALL FREQUENCIES
# ============================================================
cursor.execute(f"""
    SELECT DATE, PRICE
    FROM VALUFLOW.RAW.SP500_PRICES
    WHERE DATE >= '{START_DATE}'
    AND DATE <= '{end_date_resolved}'
    ORDER BY DATE ASC
""")
market_rows = cursor.fetchall()
print(f"S&P 500 raw price rows loaded: {len(market_rows)}\n")

# ============================================================
# 9. OUTER LOOP — FREQUENCY | INNER LOOP — TICKER
# ============================================================
all_results   = []
total_success = 0
total_skip    = 0

for FREQUENCY in FREQUENCIES:

    FREQUENCY_PERIODS  = FREQ_MAP.get(FREQUENCY, 12)
    market_return_dict = build_return_dict(market_rows, 0, 1, FREQUENCY)

    print("=" * 65)
    print(f"Frequency Pass: {FREQUENCY.upper()}")
    print(f"S&P 500 return observations: {len(market_return_dict)}")
    print("=" * 65)

    results       = []
    success_count = 0
    skip_count    = 0

    for ticker, company_name, sector, industry in universe_rows:
        notes = ""
        try:
            cursor.execute(f"""
                SELECT PRICE_DATE, ADJ_CLOSE
                FROM VALUFLOW.STAGING.PRICE_DATA
                WHERE TICKER = '{ticker}'
                AND PRICE_DATE >= '{START_DATE}'
                AND PRICE_DATE <= '{end_date_resolved}'
                ORDER BY PRICE_DATE ASC
            """)
            stock_rows = cursor.fetchall()

            if len(stock_rows) < MIN_OBSERVATIONS:
                skip_count += 1
                print(f"  SKIP  {ticker:<8} — insufficient price data ({len(stock_rows)} rows)")
                continue

            stock_return_dict = build_return_dict(stock_rows, 0, 1, FREQUENCY)
            common_periods    = sorted(set(stock_return_dict.keys()) & set(market_return_dict.keys()))

            if len(common_periods) < MIN_OBSERVATIONS:
                skip_count += 1
                print(f"  SKIP  {ticker:<8} — insufficient aligned observations ({len(common_periods)})")
                continue

            Stock_Returns  = np.array([stock_return_dict[p]  for p in common_periods])
            Market_Returns = np.array([market_return_dict[p] for p in common_periods])
            observations   = len(common_periods)

            cursor.execute(f"""
                SELECT TOTAL_DEBT, TOTAL_STOCKHOLDERS_EQUITY
                FROM VALUFLOW.STAGING.BALANCE_SHEET_ANNUAL
                WHERE TICKER = '{ticker}'
                AND TOTAL_DEBT IS NOT NULL
                AND TOTAL_STOCKHOLDERS_EQUITY IS NOT NULL
                AND TOTAL_STOCKHOLDERS_EQUITY != 0
                ORDER BY PERIOD_DATE DESC
                LIMIT 1
            """)
            bs_row = cursor.fetchone()

            if bs_row is None:
                total_debt      = None
                total_equity    = None
                debt_to_equity  = None
                beta_unlevered  = None
                beta_distortion = None
                beta_dist_pct   = None
                notes           = "No valid balance sheet data — unlevered beta skipped"
            else:
                total_debt     = float(bs_row[0])
                total_equity   = float(bs_row[1])
                debt_to_equity = total_debt / total_equity if total_equity != 0 else None

            slope, intercept, r_value, p_value, std_err = stats.linregress(Market_Returns, Stock_Returns)
            beta_levered = slope
            r_squared    = r_value ** 2

            # -- Hamada unlevered beta
            if total_equity is not None and debt_to_equity is not None:
                beta_unlevered = beta_levered / (1 + (1 - TAX_RATE) * (total_debt / total_equity))

                # -- Distortion: how much leverage inflates perceived market risk vs true business risk
                # -- Positive = leverage is inflating beta above what the business alone would imply
                # -- Negative = rare, occurs when equity is negative (distressed firms)
                beta_distortion = beta_levered - beta_unlevered

                # -- Percentage distortion: normalizes the gap relative to unlevered beta
                # -- Allows fair comparison across industries with different beta magnitudes
                beta_dist_pct = (beta_distortion / abs(beta_unlevered)) if beta_unlevered != 0 else None
            else:
                beta_unlevered  = None
                beta_distortion = None
                beta_dist_pct   = None

            alpha            = intercept
            risk_free_period = RISK_FREE_RATE / FREQUENCY_PERIODS
            alpha_benchmark  = risk_free_period * (1 - beta_levered)

            if alpha > alpha_benchmark:
                alpha_vs_benchmark = "Outperformed"
            elif alpha < alpha_benchmark:
                alpha_vs_benchmark = "Underperformed"
            else:
                alpha_vs_benchmark = "As Expected"

            results.append({
                # -- Run identity — links every row back to a specific execution
                "RUN_ID":                RUN_ID,
                "RUN_TIMESTAMP":         RUN_TIMESTAMP.strftime('%Y-%m-%d %H:%M:%S'),
                "RUN_DATE":              date.today(),
                "TICKER":                ticker,
                "COMPANY_NAME":          company_name,
                "SECTOR":                sector,
                "INDUSTRY":              industry,
                "FREQUENCY":             FREQUENCY,
                "START_DATE":            date.fromisoformat(START_DATE),
                "END_DATE":              date.fromisoformat(end_date_resolved),
                "OBSERVATIONS":          observations,
                "BETA_LEVERED":          round(beta_levered, 6),
                "BETA_UNLEVERED":        round(beta_unlevered, 6) if beta_unlevered is not None else None,
                # -- Distortion columns — core of the leverage hypothesis analysis
                "BETA_DISTORTION":       round(beta_distortion, 6) if beta_distortion is not None else None,
                "BETA_DISTORTION_PCT":   round(beta_dist_pct, 6)   if beta_dist_pct   is not None else None,
                "ALPHA":                 round(alpha, 6),
                "ALPHA_BENCHMARK":       round(alpha_benchmark, 6),
                "ALPHA_VS_BENCHMARK":    alpha_vs_benchmark,
                "R_SQUARED":             round(r_squared, 4),
                "FIRM_SPECIFIC_RISK":    round(1 - r_squared, 4),
                "STD_ERROR":             round(std_err, 6),
                "P_VALUE":               round(p_value, 6),
                "RISK_FREE_RATE_ANNUAL": round(RISK_FREE_RATE, 6),
                "TAX_RATE":              TAX_RATE,
                "TOTAL_DEBT":            total_debt,
                "TOTAL_EQUITY":          total_equity,
                "DEBT_TO_EQUITY":        round(debt_to_equity, 4) if debt_to_equity is not None else None,
                "NOTES":                 notes,
            })

            beta_u_str   = f"{beta_unlevered:.4f}"  if beta_unlevered  is not None else "N/A     "
            beta_d_str   = f"{beta_distortion:.4f}" if beta_distortion is not None else "N/A     "
            print(f"  OK    {ticker:<8} | Beta(L): {beta_levered:.4f} | Beta(U): {beta_u_str} | Distortion: {beta_d_str} | R²: {r_squared:.4f} | n={observations}")
            success_count += 1

        except Exception as e:
            skip_count += 1
            print(f"  ERROR {ticker:<8} — {e}")
            traceback.print_exc()
            try:
                cursor.close()
            except Exception:
                pass
            cursor = conn.cursor()

    print(f"\n  {FREQUENCY.upper()} pass complete — Success: {success_count} | Skipped/Errors: {skip_count}\n")
    all_results.extend(results)
    total_success += success_count
    total_skip    += skip_count

# ============================================================
# 10. WRITE ALL RESULTS TO VFMODELS
# ============================================================
print("=" * 65)
print(f"All passes complete — Total Success: {total_success} | Total Skipped/Errors: {total_skip}")

if all_results:
    df_results = pd.DataFrame(all_results)

    conn_vf = snowflake.connector.connect(
        user=        os.getenv("SNOWFLAKE_USER"),
        account=     os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=   os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=    "VFMODELS",
        schema=      "BETA",
        private_key= private_key_bytes,
        role=        "SYSADMIN",
    )
    success, nchunks, nrows, _ = write_pandas(
        conn_vf,
        df_results,
        table_name="BETA_SENSITIVITY_REGRESSION_FLOW_V1",
        database="VFMODELS",
        schema="BETA",
        overwrite=True,
        auto_create_table=False,
    )
    conn_vf.close()
    print(f"Rows written to VFMODELS.BETA.BETA_SENSITIVITY_REGRESSION_FLOW_V1: {nrows:,}")
else:
    print("No results to write.")

cursor.close()
conn.close()

print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 65)