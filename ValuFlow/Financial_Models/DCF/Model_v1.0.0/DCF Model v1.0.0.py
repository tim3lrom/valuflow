import os
os.chdir(os.path.dirname(os.path.abspath(__file__)))
import pandas as pd

df = pd.read_excel('DCF Financials.xlsx')

# Units Assumption (in millions)
units = 1_000_000

df['Revenue'] = df['Revenue'] * units

# Store the last actual year before projections
last_input_year = df['Year'].iloc[-1]

# Calculate YoY growth rates
df['Revenue_Growth'] = df['Revenue'].pct_change()
df['EBIT_Margin_Growth'] = df['EBIT_Margin'].pct_change()

# Take the average of all historical growth rates
avg_revenue_growth = df['Revenue_Growth'].mean()
avg_ebit_margin_growth = df['EBIT_Margin_Growth'].mean()

# WACC Assumption (in future, calculcate field, basically it will need to be its own calculation)
WACC = 0.10

# Terminal Growth Rate Assumption
Terminal_Growth_Rate = 0.03

# Number of projection years
projection_years = 5

# Shares Outstanding
Shares_Outstanding = 100000000

# Loop and append projection rows
for i in range(projection_years):
    new_revenue = df['Revenue'].iloc[-1] * (1 + avg_revenue_growth)
    new_ebit_margin = df['EBIT_Margin'].iloc[-1] * (1 + avg_ebit_margin_growth)

    new_row = pd.DataFrame([{
        'Year': df['Year'].iloc[-1] + 1,
        'Revenue': new_revenue,
        'EBIT_Margin': new_ebit_margin,
        'Tax_Rate': df['Tax_Rate'].iloc[-1]
    }])

    df = pd.concat([df, new_row], ignore_index=True)

# D&A Column Calculation (in future, use raw financials)
df['D&A'] = df['Revenue'] * 0.03

# CapEx Column Calculation (in future, use raw financials)
df['CapEx'] = df['Revenue'] * 0.05

# Change in NWC (in future, use raw financials)
df['NWC'] = df['Revenue'] * 0.01

# Tax Expense Column Calculation (in future, use raw financials)
df['Tax_Expense'] = df['Revenue'] * df['Tax_Rate']

# EBIT Column Calculcation (in future, remove calculcation and insert real EBIT, remove EBIT_Margin & EBIT_Margin_Growth)
df['EBIT'] = df['Revenue'] * df['EBIT_Margin']

# NOPAT Column Calculcation (in future, use raw financials when possible)
df['NOPAT'] = df['EBIT'] - df['Tax_Expense']

# D&A_Change Column Calculcation
df['D&A_Change'] = df['D&A'].diff()

# CapEx_Change Column Calculation
df['CapEx_Change'] = df['CapEx'].diff()

# NWC_Change Column Calculcation
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