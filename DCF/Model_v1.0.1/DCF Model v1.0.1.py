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

query = """
    SELECT YR, REVENUE, EBIT, INCOME_TAXES, PRETAX_INCOME, DILUTED_SHARES
    FROM VALUFLOW.INCOME_ST.YEARLY
    ORDER BY YR ASC
"""

df = pd.read_sql(query, conn)
conn.close()

df.columns = ['Year', 'Revenue', 'EBIT', 'Income_Taxes', 'Pretax_Income', 'Diluted_Shares']
df = df[df['Revenue'].notna() & (df['Revenue'] > 0)]
df = df.sort_values('Year', ascending=True).reset_index(drop=True)

# Calculate margins from raw data
df['EBIT_Margin'] = df['EBIT'] / df['Revenue']
df['Tax_Rate'] = df['Income_Taxes'] / df['Pretax_Income']

# Shares Outstanding from Snowflake (use last actual year)
Shares_Outstanding = df['Diluted_Shares'].iloc[-1]

# Store the last actual year before projections
last_input_year = df['Year'].iloc[-1]

# Calculate YoY growth rates
df['Revenue_Growth'] = df['Revenue'].pct_change().replace([float('inf'), float('-inf')], pd.NA)
df['EBIT_Margin_Growth'] = df['EBIT_Margin'].pct_change().replace([float('inf'), float('-inf')], pd.NA)

# Take the average of all historical growth rates
avg_revenue_growth = df['Revenue_Growth'].dropna().mean()
avg_ebit_margin_growth = df['EBIT_Margin_Growth'].dropna().mean()

# WACC Assumption (in future, calculate field, basically it will need to be its own calculation)
WACC = 0.10

# Terminal Growth Rate Assumption
Terminal_Growth_Rate = 0.03

# Number of projection years
projection_years = 10

# Loop and append projection rows
for i in range(projection_years):
    new_revenue = df['Revenue'].iloc[-1] * (1 + avg_revenue_growth)
    new_ebit_margin = df['EBIT_Margin'].iloc[-1] * (1 + avg_ebit_margin_growth)
    new_tax_rate = df['Tax_Rate'].iloc[-1]

    new_row = pd.DataFrame([{
        'Year': df['Year'].iloc[-1] + 1,
        'Revenue': new_revenue,
        'EBIT_Margin': new_ebit_margin,
        'Tax_Rate': new_tax_rate,
        'EBIT': new_revenue * new_ebit_margin,
        'Income_Taxes': None,
        'Pretax_Income': None,
        'Diluted_Shares': df['Diluted_Shares'].iloc[-1]
    }])

    df = pd.concat([df, new_row], ignore_index=True)

# D&A Column Calculation (in future, use raw financials)
df['D&A'] = df['Revenue'] * 0.03

# CapEx Column Calculation (in future, use raw financials)
df['CapEx'] = df['Revenue'] * 0.05

# Change in NWC (in future, use raw financials)
df['NWC'] = df['Revenue'] * 0.01

# Tax Expense Column Calculation
df['Tax_Expense'] = df['Revenue'] * df['Tax_Rate']

# EBIT Column Calculation
df['EBIT_Calc'] = df['Revenue'] * df['EBIT_Margin']

# NOPAT Column Calculation
df['NOPAT'] = df['EBIT_Calc'] - df['Tax_Expense']

# D&A_Change Column Calculation
df['D&A_Change'] = df['D&A'].diff()

# CapEx_Change Column Calculation
df['CapEx_Change'] = df['CapEx'].diff()

# NWC_Change Column Calculation
df['NWC_Change'] = df['NWC'].diff()

# FCF Column Calculation
df['FCF'] = df['NOPAT'] + df['D&A'] - df['CapEx'] - df['NWC_Change']

# Terminal Value Calculation (always uses last year's FCF)
Terminal_Value = df['FCF'].iloc[-1] * (1 + Terminal_Growth_Rate) / (WACC - Terminal_Growth_Rate)

# Discount Factor (only applied to projection years)
df['Period'] = df['Year'] - last_input_year
df['Discount_Factor'] = df['Period'].apply(lambda x: 1 / (1 + WACC) ** x if x > 0 else None)

# Discounted FCF
df['DCF'] = df['FCF'] * df['Discount_Factor']

# Discounted Terminal Value
Discounted_Terminal_Value = Terminal_Value * df['Discount_Factor'].iloc[-1]

# PV of Projected FCFs
PV_Projected_FCFs = df['DCF'].sum()

# Enterprise Value
EV = PV_Projected_FCFs + Discounted_Terminal_Value

# Implied Share Price
Implied_Share_Price = EV / Shares_Outstanding

print(df.drop(columns=['Revenue_Growth', 'EBIT_Margin_Growth']))
print(f'\nTerminal Value: {Terminal_Value:,.2f}')
print(f'Discounted Terminal Value: {Discounted_Terminal_Value:,.2f}')
print(f'PV of Projected FCFs: {PV_Projected_FCFs:,.2f}')
print(f'PV of Terminal Value: {Discounted_Terminal_Value:,.2f}')
print(f'Enterprise Value: {EV:,.2f}')
print(f'Implied Share Price: {Implied_Share_Price:,.2f}')

# Export DataFrame to CSV
df.drop(columns=['Revenue_Growth', 'EBIT_Margin_Growth']).to_csv('DCF_Output.csv', index=False)

# Export summary to text file
with open('DCF_Summary.txt', 'w') as f:
    f.write(f'Terminal Value: {Terminal_Value:,.2f}\n')
    f.write(f'Discounted Terminal Value: {Discounted_Terminal_Value:,.2f}\n')
    f.write(f'PV of Projected FCFs: {PV_Projected_FCFs:,.2f}\n')
    f.write(f'PV of Terminal Value: {Discounted_Terminal_Value:,.2f}\n')
    f.write(f'Enterprise Value: {EV:,.2f}\n')
    f.write(f'Implied Share Price: {Implied_Share_Price:,.2f}\n')

print('Output saved to DCF_Output.csv and DCF_Summary.txt')