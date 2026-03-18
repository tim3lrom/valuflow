import os
os.chdir(os.path.dirname(os.path.abspath(__file__)))
import pandas as pd
from sqlalchemy import create_engine
from pathlib import Path
import os
from dotenv import load_dotenv

# Always load .env from the same folder as this script, regardless of where
# Python is called from — fixes "Password is empty" when running from PowerShell.
load_dotenv(dotenv_path=Path(__file__).parent / ".env")

# ══════════════════════════════════════════════════════════════════════════════
# USER CONTROLS
# ══════════════════════════════════════════════════════════════════════════════
# For each assumption, set the year range you want to look back over.
# Set YEAR_START / YEAR_END to None to use all available history.
#
# OVERRIDE: If you want to skip historical derivation entirely and just plug in
# a number, set the override value directly (e.g. REVENUE_GROWTH_OVERRIDE = 0.05
# for 5%). Leave as None to auto-derive from the lookback window.
# ══════════════════════════════════════════════════════════════════════════════

# ── Revenue Growth ────────────────────────────────────────────────────────────
REVENUE_GROWTH_START    = 2018   # First year of lookback window
REVENUE_GROWTH_END      = None   # Last year of lookback window (None = latest)
REVENUE_GROWTH_OVERRIDE = None   # e.g. 0.05 → forces 5% growth regardless of history

# ── EBIT Margin ───────────────────────────────────────────────────────────────
EBIT_MARGIN_START       = 2018
EBIT_MARGIN_END         = None
EBIT_MARGIN_OVERRIDE    = None   # e.g. 0.25 → forces 25% EBIT margin

# ── D&A as % of Revenue ───────────────────────────────────────────────────────
DA_START                = 2018
DA_END                  = None
DA_PCT_OVERRIDE         = None   # e.g. 0.04 → forces 4% of revenue

# ── CapEx as % of Revenue ─────────────────────────────────────────────────────
CAPEX_START             = 2018
CAPEX_END               = None
CAPEX_PCT_OVERRIDE      = None   # e.g. 0.05 → forces 5% of revenue

# ── NWC Change as % of Revenue ────────────────────────────────────────────────
# NWC change is computed as (change in NWC / revenue) for each year, then averaged.
# A positive number = working capital is a use of cash (subtracts from FCF).
# A negative number = working capital is a source of cash (adds to FCF).
# KO runs negative NWC (liabilities > assets), so this is often slightly negative.
NWC_START               = 2018
NWC_END                 = None
NWC_PCT_OVERRIDE        = None   # e.g. 0.01 → forces 1% of revenue per year
NWC_SWING_CAP           = 0.05   # Caps individual-year NWC/revenue ratios at ±5%
                                  # before averaging, to filter reclassification events.

# ── Tax Rate ──────────────────────────────────────────────────────────────────
TAX_RATE_START          = 2018
TAX_RATE_END            = None
TAX_RATE_OVERRIDE       = None   # e.g. 0.21 → forces 21% effective tax rate
TAX_RATE_MIN            = 0.05   # Winsorize floor (filters one-time tax benefits)
TAX_RATE_MAX            = 0.40   # Winsorize ceiling (filters one-time charges like TCJA)

# ── DCF Model Parameters ──────────────────────────────────────────────────────
WACC                    = 0.07   # Weighted Average Cost of Capital
TERMINAL_GROWTH_RATE    = 0.02   # Perpetuity growth rate for terminal value
PROJECTION_YEARS        = 5      # Number of years to project forward

# ══════════════════════════════════════════════════════════════════════════════
# END OF USER CONTROLS — nothing below needs to be edited for normal use
# ══════════════════════════════════════════════════════════════════════════════


# ── Helper: slice a dataframe to a year window ────────────────────────────────
def window(df, start, end):
    mask = pd.Series([True] * len(df), index=df.index)
    if start is not None:
        mask &= df['Year'] >= start
    if end is not None:
        mask &= df['Year'] <= end
    return df[mask]


# ── Snowflake Connection ──────────────────────────────────────────────────────
account   = os.getenv('SNOWFLAKE_ACCOUNT')
user      = os.getenv('SNOWFLAKE_USER')
password  = os.getenv('SNOWFLAKE_PASSWORD')
database  = os.getenv('SNOWFLAKE_DATABASE')
warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
schema    = os.getenv('SNOWFLAKE_SCHEMA', 'INCOME_ST')

connection_string = (
    f"snowflake://{user}:{password}@{account}/"
    f"{database}/{schema}?warehouse={warehouse}"
)
engine = create_engine(connection_string)

# Pull income statement data
query_inc = """
    SELECT YR, REVENUE, EBIT, INCOME_TAXES, PRETAX_INCOME, DILUTED_SHARES
    FROM VALUFLOW.INCOME_ST.YEARLY
    ORDER BY YR ASC
"""
df_inc = pd.read_sql(query_inc, engine)
df_inc.columns = ['Year', 'Revenue', 'EBIT', 'Income_Taxes', 'Pretax_Income', 'Diluted_Shares']

# Pull cash flow data
query_cf = """
    SELECT YR, D_A, NET_CHANGE_PPE
    FROM VALUFLOW.CASH_FLOW.YEARLY
    ORDER BY YR ASC
"""
df_cf = pd.read_sql(query_cf, engine)
df_cf.columns = ['Year', 'D&A_Raw', 'CapEx_Raw']

# Pull balance sheet data
query_bs = """
    SELECT YR, TOTAL_CURRENT_ASSETS, TOTAL_CURRENT_LIABILITIES
    FROM VALUFLOW.BALANCE.YEARLY
    ORDER BY YR ASC
"""
df_bs = pd.read_sql(query_bs, engine)
df_bs.columns = ['Year', 'Current_Assets', 'Current_Liabilities']

engine.dispose()

# ── Merge & clean ─────────────────────────────────────────────────────────────
df = pd.merge(df_inc, df_cf, on='Year', how='left')
df = pd.merge(df, df_bs, on='Year', how='left')
df = df[df['Revenue'].notna() & (df['Revenue'] > 0)]
df = df.sort_values('Year', ascending=True).reset_index(drop=True)

# Force CapEx and D&A positive across the full frame before any windowing
df['D&A_Raw']  = df['D&A_Raw'].abs()
df['CapEx_Raw'] = df['CapEx_Raw'].abs()

# Compute derived columns on the full frame so windowed slices inherit them
df['EBIT_Margin'] = df['EBIT'] / df['Revenue']
df['Tax_Rate']    = (df['Income_Taxes'] / df['Pretax_Income']).clip(TAX_RATE_MIN, TAX_RATE_MAX)
df['NWC']         = (df['Current_Assets'] - df['Current_Liabilities']).ffill()
df['NWC_Change']  = df['NWC'].diff()

# Shares and last actual year come from the full unfiltered frame
Shares_Outstanding = df['Diluted_Shares'].iloc[-1]
last_input_year    = df['Year'].iloc[-1]

# ── Derive each assumption from its own lookback window ───────────────────────

# Revenue Growth
if REVENUE_GROWTH_OVERRIDE is not None:
    avg_revenue_growth = REVENUE_GROWTH_OVERRIDE
    rev_source = "manual override"
else:
    w = window(df, REVENUE_GROWTH_START, REVENUE_GROWTH_END)
    avg_revenue_growth = w['Revenue'].pct_change().replace([float('inf'), float('-inf')], pd.NA).dropna().mean()
    rev_source = f"{int(w['Year'].iloc[0])}–{int(w['Year'].iloc[-1])}"

# EBIT Margin — averaged over window, held flat in projections
if EBIT_MARGIN_OVERRIDE is not None:
    base_ebit_margin = EBIT_MARGIN_OVERRIDE
    margin_source = "manual override"
else:
    w = window(df, EBIT_MARGIN_START, EBIT_MARGIN_END)
    base_ebit_margin = w['EBIT_Margin'].mean()
    margin_source = f"{int(w['Year'].iloc[0])}–{int(w['Year'].iloc[-1])} avg"

# D&A %
if DA_PCT_OVERRIDE is not None:
    da_pct = DA_PCT_OVERRIDE
    da_source = "manual override"
else:
    w = window(df, DA_START, DA_END)
    w = w[w['D&A_Raw'] != 0]
    da_pct = (w['D&A_Raw'] / w['Revenue']).mean()
    da_source = f"{int(w['Year'].iloc[0])}–{int(w['Year'].iloc[-1])}"

# CapEx %
if CAPEX_PCT_OVERRIDE is not None:
    capex_pct = CAPEX_PCT_OVERRIDE
    capex_source = "manual override"
else:
    w = window(df, CAPEX_START, CAPEX_END)
    w = w[w['CapEx_Raw'] != 0]
    capex_pct = (w['CapEx_Raw'] / w['Revenue']).mean()
    capex_source = f"{int(w['Year'].iloc[0])}–{int(w['Year'].iloc[-1])}"

# NWC Change % of Revenue
if NWC_PCT_OVERRIDE is not None:
    nwc_pct = NWC_PCT_OVERRIDE
    nwc_source = "manual override"
else:
    w = window(df, NWC_START, NWC_END)
    w = w[w['NWC_Change'].notna() & (w['NWC_Change'] != 0)]
    nwc_pct_series = (w['NWC_Change'] / w['Revenue']).clip(-NWC_SWING_CAP, NWC_SWING_CAP)
    nwc_pct = 0 if nwc_pct_series.empty else nwc_pct_series.mean()
    nwc_source = f"{int(w['Year'].iloc[0])}–{int(w['Year'].iloc[-1])} (capped ±{NWC_SWING_CAP:.0%})"

# Tax Rate
if TAX_RATE_OVERRIDE is not None:
    base_tax_rate = TAX_RATE_OVERRIDE
    tax_source = "manual override"
else:
    w = window(df, TAX_RATE_START, TAX_RATE_END)
    base_tax_rate = w['Tax_Rate'].mean()
    tax_source = f"{int(w['Year'].iloc[0])}–{int(w['Year'].iloc[-1])} avg (winsorized {TAX_RATE_MIN:.0%}–{TAX_RATE_MAX:.0%})"

# ── Sanity check ──────────────────────────────────────────────────────────────
print("=" * 62)
print("PROJECTION ASSUMPTIONS")
print("=" * 62)
print(f"  Revenue Growth:      {avg_revenue_growth:.2%}  [{rev_source}]")
print(f"  EBIT Margin:         {base_ebit_margin:.2%}  [{margin_source}]")
print(f"  D&A % of Revenue:    {da_pct:.2%}  [{da_source}]")
print(f"  CapEx % of Revenue:  {capex_pct:.2%}  [{capex_source}]")
print(f"  NWC Change %:        {nwc_pct:.2%}  [{nwc_source}]")
print(f"  Tax Rate:            {base_tax_rate:.2%}  [{tax_source}]")
print(f"  WACC:                {WACC:.2%}")
print(f"  Terminal Growth:     {TERMINAL_GROWTH_RATE:.2%}")
print(f"  Projection Years:    {PROJECTION_YEARS}")
print(f"  Shares Outstanding:  {Shares_Outstanding:,.0f}")
print()

# ── Build projection dataframe from last actual year ──────────────────────────
df_proj = df.copy()

for i in range(PROJECTION_YEARS):
    prev_revenue   = df_proj['Revenue'].iloc[-1]
    new_revenue    = prev_revenue * (1 + avg_revenue_growth)
    new_ebit       = new_revenue * base_ebit_margin
    new_nwc_change = new_revenue * nwc_pct

    new_row = pd.DataFrame([{
        'Year':                df_proj['Year'].iloc[-1] + 1,
        'Revenue':             new_revenue,
        'EBIT':                new_ebit,
        'Income_Taxes':        None,
        'Pretax_Income':       None,
        'Diluted_Shares':      df_proj['Diluted_Shares'].iloc[-1],
        'D&A_Raw':             new_revenue * da_pct,
        'CapEx_Raw':           new_revenue * capex_pct,
        'Current_Assets':      None,
        'Current_Liabilities': None,
        'EBIT_Margin':         base_ebit_margin,
        'Tax_Rate':            base_tax_rate,
        'NWC':                 None,
        'NWC_Change':          new_nwc_change,
    }])

    df_proj = pd.concat([df_proj, new_row], ignore_index=True)

# ── FCF Calculation ───────────────────────────────────────────────────────────
# EBIT_Calc must be computed before Tax_Expense references it
df_proj['D&A']         = df_proj['D&A_Raw'].abs()
df_proj['CapEx']       = df_proj['CapEx_Raw'].abs()
df_proj['EBIT_Calc']   = df_proj['Revenue'] * df_proj['EBIT_Margin']
df_proj['Tax_Expense'] = df_proj['EBIT_Calc'] * df_proj['Tax_Rate']  # tax on EBIT, not revenue
df_proj['NOPAT']       = df_proj['EBIT_Calc'] - df_proj['Tax_Expense']
df_proj['FCF']         = df_proj['NOPAT'] + df_proj['D&A'] - df_proj['CapEx'] - df_proj['NWC_Change']

# ── Discounting ───────────────────────────────────────────────────────────────
df_proj['Period']          = df_proj['Year'] - last_input_year
df_proj['Discount_Factor'] = df_proj['Period'].apply(
    lambda x: 1 / (1 + WACC) ** x if x > 0 else None
)
df_proj['DCF'] = df_proj['FCF'] * df_proj['Discount_Factor']

# ── Valuation ─────────────────────────────────────────────────────────────────
Terminal_Value            = df_proj['FCF'].iloc[-1] * (1 + TERMINAL_GROWTH_RATE) / (WACC - TERMINAL_GROWTH_RATE)
Discounted_Terminal_Value = Terminal_Value * df_proj['Discount_Factor'].iloc[-1]
PV_Projected_FCFs         = df_proj.loc[df_proj['Period'] > 0, 'DCF'].sum()
EV                        = PV_Projected_FCFs + Discounted_Terminal_Value
Implied_Share_Price       = EV / Shares_Outstanding

# ── Output ────────────────────────────────────────────────────────────────────
print("=" * 62)
print("VALUATION OUTPUT")
print("=" * 62)
print(f"  Terminal Value:             {Terminal_Value:>20,.2f}")
print(f"  Discounted Terminal Value:  {Discounted_Terminal_Value:>20,.2f}")
print(f"  PV of Projected FCFs:       {PV_Projected_FCFs:>20,.2f}")
print(f"  Enterprise Value:           {EV:>20,.2f}")
print(f"  Implied Share Price:        ${Implied_Share_Price:>19,.2f}")
print()

# ── Export ────────────────────────────────────────────────────────────────────
df_proj.to_csv('DCF_Output.csv', index=False)

with open('DCF_Summary.txt', 'w') as f:
    f.write("=" * 62 + "\n")
    f.write("PROJECTION ASSUMPTIONS\n")
    f.write("=" * 62 + "\n")
    f.write(f"  Revenue Growth:      {avg_revenue_growth:.2%}  [{rev_source}]\n")
    f.write(f"  EBIT Margin:         {base_ebit_margin:.2%}  [{margin_source}]\n")
    f.write(f"  D&A % of Revenue:    {da_pct:.2%}  [{da_source}]\n")
    f.write(f"  CapEx % of Revenue:  {capex_pct:.2%}  [{capex_source}]\n")
    f.write(f"  NWC Change %:        {nwc_pct:.2%}  [{nwc_source}]\n")
    f.write(f"  Tax Rate:            {base_tax_rate:.2%}  [{tax_source}]\n")
    f.write(f"  WACC:                {WACC:.2%}\n")
    f.write(f"  Terminal Growth:     {TERMINAL_GROWTH_RATE:.2%}\n")
    f.write(f"  Projection Years:    {PROJECTION_YEARS}\n")
    f.write(f"  Shares Outstanding:  {Shares_Outstanding:,.0f}\n\n")
    f.write("=" * 62 + "\n")
    f.write("VALUATION OUTPUT\n")
    f.write("=" * 62 + "\n")
    f.write(f"  Terminal Value:             {Terminal_Value:>20,.2f}\n")
    f.write(f"  Discounted Terminal Value:  {Discounted_Terminal_Value:>20,.2f}\n")
    f.write(f"  PV of Projected FCFs:       {PV_Projected_FCFs:>20,.2f}\n")
    f.write(f"  Enterprise Value:           {EV:>20,.2f}\n")
    f.write(f"  Implied Share Price:        ${Implied_Share_Price:>19,.2f}\n")

print("Output saved to DCF_Output.csv and DCF_Summary.txt")