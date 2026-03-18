import os
os.chdir(os.path.dirname(os.path.abspath(__file__)))
import pandas as pd
import snowflake.connector
import os
from dotenv import load_dotenv

load_dotenv()

conn = snowflake.connector.connect(
    user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'),
    account=os.getenv('SNOWFLAKE_ACCOUNT'),
    warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
    database=os.getenv('SNOWFLAKE_DATABASE'),
    schema='INCOME_ST'
)

# Pull income statement data
query_inc = """
    SELECT YR, REVENUE, EBIT, INCOME_TAXES, PRETAX_INCOME, DILUTED_SHARES
    FROM VALUFLOW.INCOME_ST.YEARLY
    ORDER BY YR ASC
"""
df_inc = pd.read_sql(query_inc, conn)
df_inc.columns = ['Year', 'Revenue', 'EBIT', 'Income_Taxes', 'Pretax_Income', 'Diluted_Shares']

# Pull cash flow data
query_cf = """
    SELECT YR, D_A, NET_CHANGE_PPE
    FROM VALUFLOW.CASH_FLOW.YEARLY
    ORDER BY YR ASC
"""
df_cf = pd.read_sql(query_cf, conn)
df_cf.columns = ['Year', 'D&A_Raw', 'CapEx_Raw']

# Pull balance sheet data
query_bs = """
     SELECT YR, TOTAL_CURRENT_ASSETS, TOTAL_CURRENT_LIABILITIES
    FROM VALUFLOW.BALANCE.YEARLY
    ORDER BY YR ASC
"""
df_bs = pd.read_sql(query_bs, conn)
df_bs.columns = ['Year', 'Current_Assets', 'Current_Liabilities']

conn.close()

# Merge all three tables on Year
df = pd.merge(df_inc, df_cf, on='Year', how='left')
df = pd.merge(df, df_bs, on='Year', how='left')

# Clean data - remove rows where Revenue is missing or zero
df = df[df['Revenue'].notna() & (df['Revenue'] > 0)]
df = df.sort_values('Year', ascending=True).reset_index(drop=True)

# ── FIX 1: Restrict historical window to 2018–last available year ─────────────
# Excludes Tax Reform outlier (2017), pre-modern capex structure, and noisy
# early NWC data. Adjust HISTORY_START if needed.
HISTORY_START = 2018
df_hist = df[df['Year'] >= HISTORY_START].copy()

# ── Calculate margins from raw data ──────────────────────────────────────────
df_hist['EBIT_Margin'] = df_hist['EBIT'] / df_hist['Revenue']

# FIX 2: Winsorize tax rate — cap at a sensible range (5%–40%) before averaging
#         This neutralizes one-time events like the 2017 TCJA repatriation charge.
df_hist['Tax_Rate'] = (df_hist['Income_Taxes'] / df_hist['Pretax_Income']).clip(0.05, 0.40)

# Calculate NWC and NWC change from balance sheet
df_hist['NWC'] = df_hist['Current_Assets'] - df_hist['Current_Liabilities']
df_hist['NWC'] = df_hist['NWC'].ffill()
df_hist['NWC_Change'] = df_hist['NWC'].diff()

# Shares Outstanding — convert from raw units to millions for readability,
# but keep raw for the final share price division (EV is also in raw dollars)
Shares_Outstanding = df_hist['Diluted_Shares'].iloc[-1]

# Store the last actual year before projections
last_input_year = df_hist['Year'].iloc[-1]

# ── FIX 3: CapEx sign — force positive after taking abs of NET_CHANGE_PPE ───
#           capex_pct is derived only from non-zero rows, now guaranteed positive
df_hist['D&A_Raw'] = df_hist['D&A_Raw'].abs()
df_hist['CapEx_Raw'] = df_hist['CapEx_Raw'].abs()

da_pct   = (df_hist[df_hist['D&A_Raw']   != 0]['D&A_Raw']   / df_hist[df_hist['D&A_Raw']   != 0]['Revenue']).mean()
capex_pct = (df_hist[df_hist['CapEx_Raw'] != 0]['CapEx_Raw'] / df_hist[df_hist['CapEx_Raw'] != 0]['Revenue']).mean()

# ── FIX 4: Winsorize NWC changes before computing the percentage ─────────────
#           Large balance-sheet reclassification events (e.g. 2018) would otherwise
#           blow up the NWC assumption. Cap at ±5% of revenue for each year.
nwc_valid = df_hist[df_hist['NWC_Change'].notna() & (df_hist['NWC_Change'] != 0)].copy()
nwc_pct_series = nwc_valid['NWC_Change'] / nwc_valid['Revenue']
nwc_pct_series = nwc_pct_series.clip(-0.05, 0.05)   # cap individual-year swings
nwc_pct = 0 if nwc_pct_series.empty else nwc_pct_series.mean()

# Calculate YoY growth rates on the cleaned historical window
df_hist['Revenue_Growth']     = df_hist['Revenue'].pct_change().replace([float('inf'), float('-inf')], pd.NA)
df_hist['EBIT_Margin_Growth'] = df_hist['EBIT_Margin'].pct_change().replace([float('inf'), float('-inf')], pd.NA)

avg_revenue_growth     = df_hist['Revenue_Growth'].dropna().mean()
avg_ebit_margin_growth = df_hist['EBIT_Margin_Growth'].dropna().mean()

# ── Sanity-check print before projections ────────────────────────────────────
print("=== Projection Assumptions ===")
print(f"  Avg Revenue Growth:      {avg_revenue_growth:.2%}")
print(f"  Avg EBIT Margin Growth:  {avg_ebit_margin_growth:.2%}")
print(f"  D&A as % of Revenue:     {da_pct:.2%}")
print(f"  CapEx as % of Revenue:   {capex_pct:.2%}")
print(f"  NWC Change as % of Rev:  {nwc_pct:.2%}")
print(f"  Shares Outstanding (raw):{Shares_Outstanding:,.0f}")
print()

# WACC Assumption (in future, calculate from CAPM/market data)
WACC = 0.07

# Terminal Growth Rate Assumption
Terminal_Growth_Rate = 0.02

# Number of projection years
projection_years = 5

# Start projections from the cleaned historical df
df = df_hist.copy()

# Loop and append projection rows
for i in range(projection_years):
    new_revenue     = df['Revenue'].iloc[-1] * (1 + avg_revenue_growth)
    new_ebit_margin = df['EBIT_Margin'].iloc[-1] * (1 + avg_ebit_margin_growth)
    new_tax_rate    = df['Tax_Rate'].iloc[-1]           # carry forward last clean year
    new_nwc         = df['NWC'].iloc[-1] * (1 + nwc_pct)
    new_nwc_change  = new_nwc - df['NWC'].iloc[-1]

    new_row = pd.DataFrame([{
        'Year':                df['Year'].iloc[-1] + 1,
        'Revenue':             new_revenue,
        'EBIT':                new_revenue * new_ebit_margin,
        'Income_Taxes':        None,
        'Pretax_Income':       None,
        'Diluted_Shares':      df['Diluted_Shares'].iloc[-1],
        'D&A_Raw':             new_revenue * da_pct,
        'CapEx_Raw':           new_revenue * capex_pct,
        'Current_Assets':      None,
        'Current_Liabilities': None,
        'NWC':                 new_nwc,
        'NWC_Change':          new_nwc_change,
        'EBIT_Margin':         new_ebit_margin,
        'Tax_Rate':            new_tax_rate
    }])

    df = pd.concat([df, new_row], ignore_index=True)

# D&A and CapEx — already abs() from above, safe to use directly
df['D&A']   = df['D&A_Raw'].abs()
df['CapEx'] = df['CapEx_Raw'].abs()

# Tax Expense
df['Tax_Expense'] = df['Revenue'] * df['Tax_Rate']

# EBIT
df['EBIT_Calc'] = df['Revenue'] * df['EBIT_Margin']

# NOPAT
df['NOPAT'] = df['EBIT_Calc'] - df['Tax_Expense']

# FCF
df['FCF'] = df['NOPAT'] + df['D&A'] - df['CapEx'] - df['NWC_Change']

# Terminal Value
Terminal_Value = df['FCF'].iloc[-1] * (1 + Terminal_Growth_Rate) / (WACC - Terminal_Growth_Rate)

# Discount Factor (only for projection years)
df['Period'] = df['Year'] - last_input_year
df['Discount_Factor'] = df['Period'].apply(lambda x: 1 / (1 + WACC) ** x if x > 0 else None)

# Discounted FCF
df['DCF'] = df['FCF'] * df['Discount_Factor']

# Discounted Terminal Value
Discounted_Terminal_Value = Terminal_Value * df['Discount_Factor'].iloc[-1]

# PV of Projected FCFs (projection years only)
PV_Projected_FCFs = df.loc[df['Period'] > 0, 'DCF'].sum()

# Enterprise Value
EV = PV_Projected_FCFs + Discounted_Terminal_Value

# ── FIX 1 (continued): Shares are in raw units, EV is in raw dollars ─────────
# No unit mismatch — both are raw. Implied price should now be meaningful.
Implied_Share_Price = EV / Shares_Outstanding

print(df.drop(columns=['Revenue_Growth', 'EBIT_Margin_Growth'], errors='ignore'))
print(f'\nTerminal Value:            {Terminal_Value:,.2f}')
print(f'Discounted Terminal Value: {Discounted_Terminal_Value:,.2f}')
print(f'PV of Projected FCFs:      {PV_Projected_FCFs:,.2f}')
print(f'PV of Terminal Value:       {Discounted_Terminal_Value:,.2f}')
print(f'Enterprise Value:           {EV:,.2f}')
print(f'Implied Share Price:         ${Implied_Share_Price:,.2f}')

# Export outputs
df.drop(columns=['Revenue_Growth', 'EBIT_Margin_Growth'], errors='ignore').to_csv('DCF_Output.csv', index=False)
with open('DCF_Summary.txt', 'w') as f:
    f.write(f'=== Projection Assumptions ===\n')
    f.write(f'Avg Revenue Growth:      {avg_revenue_growth:.2%}\n')
    f.write(f'Avg EBIT Margin Growth:  {avg_ebit_margin_growth:.2%}\n')
    f.write(f'D&A as % of Revenue:     {da_pct:.2%}\n')
    f.write(f'CapEx as % of Revenue:   {capex_pct:.2%}\n')
    f.write(f'NWC Change as % of Rev:  {nwc_pct:.2%}\n')
    f.write(f'Shares Outstanding:      {Shares_Outstanding:,.0f}\n\n')
    f.write(f'=== Valuation Output ===\n')
    f.write(f'Terminal Value:            {Terminal_Value:,.2f}\n')
    f.write(f'Discounted Terminal Value: {Discounted_Terminal_Value:,.2f}\n')
    f.write(f'PV of Projected FCFs:      {PV_Projected_FCFs:,.2f}\n')
    f.write(f'PV of Terminal Value:      {Discounted_Terminal_Value:,.2f}\n')
    f.write(f'Enterprise Value:          {EV:,.2f}\n')
    f.write(f'Implied Share Price:       ${Implied_Share_Price:,.2f}\n')

print('\nOutput saved to DCF_Output.csv and DCF_Summary.txt')