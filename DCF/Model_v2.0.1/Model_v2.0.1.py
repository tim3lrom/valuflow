import os
os.chdir(os.path.dirname(os.path.abspath(__file__)))
# ----------------------------------------------------
# CAPM Inputs ----------------------------------------
Risk_Free_Rate = 0.0423
ERP = 0.0418
Beta = 0.74
# ----------------------------------------------------
# CAPM Calculcation ----------------------------------
Cost_of_Equity = Risk_Free_Rate + Beta * ERP
Ke = Cost_of_Equity
# ----------------------------------------------------
# Cost of Debt Inputs --------------------------------
Pre_Tax_Cost_of_Debt = 0.035
Marginal_Tax_Rate = 0.21
# ----------------------------------------------------
# Cost of Debt Calculation ---------------------------
After_Tax_Cost_of_Debt = Pre_Tax_Cost_of_Debt * (1 - Marginal_Tax_Rate)
Kd = After_Tax_Cost_of_Debt
# ----------------------------------------------------
# Capital Structure Inputs ---------------------------
Market_Cap = 334680000000
Total_Debt = 45492000000
# ----------------------------------------------------
# Capital Structure Calculations ---------------------
Total_Capital = Market_Cap + Total_Debt
Weight_of_Equity = Market_Cap / Total_Capital
Weight_of_Debt = Total_Debt / Total_Capital
# ----------------------------------------------------
# WACC Calculation -----------------------------------
WACC = Weight_of_Equity * Ke + Weight_of_Debt * Kd
# ----------------------------------------------------
# Results -------------------------------------------
print("=" * 50)
print("WACC MODEL v1.0.0 RESULTS")
print("=" * 50)
print(f"Cost of Equity (Ke):     {Ke * 100:.2f}%")
print(f"Cost of Debt (Kd):       {Kd * 100:.2f}%")
print("=" * 50)
print(f"Weight of Equity:        {Weight_of_Equity * 100:.2f}%")
print(f"Weight of Debt:          {Weight_of_Debt * 100:.2f}%")
print("=" * 50)
print(f"Weight Sum Check:        {Weight_of_Equity + Weight_of_Debt}")
print("=" * 50)
print(f"WACC:                    {WACC * 100:.2f}%")
print("=" * 50)