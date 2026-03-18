import os
os.chdir(os.path.dirname(os.path.abspath(__file__)))
# Model Inputs --------------------------------------
Base_Year_Revenue = 47941000000
Revenue_Growth_Rate = 0.037
EBIT_Margin = 0.301
Tax_Rate = 0.18
DA_of_Revenue = 0.03
CapEx_of_Revenue = 0.05
NWC_Delta = 0.01
Terminal_Growth_Rate = 0.03
Net_Debt = 32000000000
Shares_Outstanding = 4313000000
# ----------------------------------------------------
# WACC Import ----------------------------------------
import importlib.util
spec = importlib.util.spec_from_file_location("wacc", r"C:\Users\timel\Desktop\ValuFlow\Python_Modeling\DCF\Model_v2.0.1\Model_v2.0.1.py")
wacc_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(wacc_module)
WACC = wacc_module.WACC
# ----------------------------------------------------
# Year Definitions -----------------------------------
Base_Year = 2025
Base_Plus_1 = Base_Year + 1
Base_Plus_2 = Base_Year + 2
Base_Plus_3 = Base_Year + 3
Base_Plus_4 = Base_Year + 4
Base_Plus_5 = Base_Year + 5
# ----------------------------------------------------
# Base Years + 1 -> 5 Revenue Calculations -----------
Base_Plus_1_Revenue = Base_Year_Revenue * (1 + Revenue_Growth_Rate)
Base_Plus_2_Revenue = Base_Plus_1_Revenue * (1 + Revenue_Growth_Rate)
Base_Plus_3_Revenue = Base_Plus_2_Revenue * (1 + Revenue_Growth_Rate)
Base_Plus_4_Revenue = Base_Plus_3_Revenue * (1 + Revenue_Growth_Rate)
Base_Plus_5_Revenue = Base_Plus_4_Revenue * (1 + Revenue_Growth_Rate)
# ----------------------------------------------------
# Base Years + 1 -> 5 EBIT Calculations --------------
Base_Plus_1_EBIT = Base_Plus_1_Revenue * EBIT_Margin
Base_Plus_2_EBIT = Base_Plus_2_Revenue * EBIT_Margin
Base_Plus_3_EBIT = Base_Plus_3_Revenue * EBIT_Margin
Base_Plus_4_EBIT = Base_Plus_4_Revenue * EBIT_Margin
Base_Plus_5_EBIT = Base_Plus_5_Revenue * EBIT_Margin
# ----------------------------------------------------
# Base Years + 1 -> 5 Tax Calculations ---------------
Base_Plus_1_Tax = Base_Plus_1_EBIT * Tax_Rate
Base_Plus_2_Tax = Base_Plus_2_EBIT * Tax_Rate
Base_Plus_3_Tax = Base_Plus_3_EBIT * Tax_Rate
Base_Plus_4_Tax = Base_Plus_4_EBIT * Tax_Rate
Base_Plus_5_Tax = Base_Plus_5_EBIT * Tax_Rate
# ----------------------------------------------------
# Base Years + 1 -> 5 NOPAT Calculations -------------
Base_Plus_1_NOPAT = Base_Plus_1_EBIT - Base_Plus_1_Tax
Base_Plus_2_NOPAT = Base_Plus_2_EBIT - Base_Plus_2_Tax
Base_Plus_3_NOPAT = Base_Plus_3_EBIT - Base_Plus_3_Tax
Base_Plus_4_NOPAT = Base_Plus_4_EBIT - Base_Plus_4_Tax
Base_Plus_5_NOPAT = Base_Plus_5_EBIT - Base_Plus_5_Tax
# ----------------------------------------------------
# Base Years + 1 -> 5 D&A Calculations ---------------
Base_Plus_1_DA = Base_Plus_1_Revenue * DA_of_Revenue
Base_Plus_2_DA = Base_Plus_2_Revenue * DA_of_Revenue
Base_Plus_3_DA = Base_Plus_3_Revenue * DA_of_Revenue
Base_Plus_4_DA = Base_Plus_4_Revenue * DA_of_Revenue
Base_Plus_5_DA = Base_Plus_5_Revenue * DA_of_Revenue
# ----------------------------------------------------
# Base Years + 1 -> 5 CapEx Calculations -------------
Base_Plus_1_CapEx = Base_Plus_1_Revenue * CapEx_of_Revenue
Base_Plus_2_CapEx = Base_Plus_2_Revenue * CapEx_of_Revenue
Base_Plus_3_CapEx = Base_Plus_3_Revenue * CapEx_of_Revenue
Base_Plus_4_CapEx = Base_Plus_4_Revenue * CapEx_of_Revenue
Base_Plus_5_CapEx = Base_Plus_5_Revenue * CapEx_of_Revenue
# ----------------------------------------------------
# Base Years + 1 -> 5 Delta NWC Calculations ---------
Base_Plus_1_Delta_NWC = Base_Plus_1_Revenue * NWC_Delta
Base_Plus_2_Delta_NWC = Base_Plus_2_Revenue * NWC_Delta
Base_Plus_3_Delta_NWC = Base_Plus_3_Revenue * NWC_Delta
Base_Plus_4_Delta_NWC = Base_Plus_4_Revenue * NWC_Delta
Base_Plus_5_Delta_NWC = Base_Plus_5_Revenue * NWC_Delta
# ----------------------------------------------------
# Base Years + 1 -> 5 FCF Calculations ---------------
Base_Plus_1_FCF = Base_Plus_1_NOPAT + Base_Plus_1_DA - Base_Plus_1_CapEx - Base_Plus_1_Delta_NWC
Base_Plus_2_FCF = Base_Plus_2_NOPAT + Base_Plus_2_DA - Base_Plus_2_CapEx - Base_Plus_2_Delta_NWC
Base_Plus_3_FCF = Base_Plus_3_NOPAT + Base_Plus_3_DA - Base_Plus_3_CapEx - Base_Plus_3_Delta_NWC
Base_Plus_4_FCF = Base_Plus_4_NOPAT + Base_Plus_4_DA - Base_Plus_4_CapEx - Base_Plus_4_Delta_NWC
Base_Plus_5_FCF = Base_Plus_5_NOPAT + Base_Plus_5_DA - Base_Plus_5_CapEx - Base_Plus_5_Delta_NWC
# ----------------------------------------------------
# Base Years + 1 -> 5 Discounted Factor Calculations -
Base_Plus_1_Discount_Factor = 1 / (1 + WACC) **1
Base_Plus_2_Discount_Factor = 1 / (1 + WACC) **2
Base_Plus_3_Discount_Factor = 1 / (1 + WACC) **3
Base_Plus_4_Discount_Factor = 1 / (1 + WACC) **4
Base_Plus_5_Discount_Factor = 1 / (1 + WACC) **5
# ----------------------------------------------------
# Base Years + 1 -> 5 Discounted FCF Calculations ----
Base_Plus_1_Discount_FCF = Base_Plus_1_FCF / (1 + WACC) **1
Base_Plus_2_Discount_FCF = Base_Plus_2_FCF / (1 + WACC) **2
Base_Plus_3_Discount_FCF = Base_Plus_3_FCF / (1 + WACC) **3
Base_Plus_4_Discount_FCF = Base_Plus_4_FCF / (1 + WACC) **4
Base_Plus_5_Discount_FCF = Base_Plus_5_FCF / (1 + WACC) **5
# ----------------------------------------------------
# Terminal Value via Perp. Growth Calculations -------
Terminal_Value_Perp_Growth = Base_Plus_5_FCF * (1 + Terminal_Growth_Rate) / (WACC - Terminal_Growth_Rate)
# ----------------------------------------------------
# Terminal Value via Perp. Growth Discounted Calculation 
Terminal_Value_Perp_Growth_Discounted = Terminal_Value_Perp_Growth * Base_Plus_5_Discount_Factor
# ----------------------------------------------------
# PV of Projected FCFs Calculations ------------------
PV_of_Projected_FCFs = Base_Plus_1_Discount_FCF + Base_Plus_2_Discount_FCF + Base_Plus_3_Discount_FCF + Base_Plus_4_Discount_FCF + Base_Plus_5_Discount_FCF
# ----------------------------------------------------
# Enterprise Value Calculation -----------------------
Enterprise_Value = PV_of_Projected_FCFs + Terminal_Value_Perp_Growth_Discounted
# ----------------------------------------------------
# Equity Value Calculation ---------------------------
Equity_Value = Enterprise_Value - Net_Debt
# ----------------------------------------------------
# Implied Share Price Calculation --------------------
Implied_Share_Price = Equity_Value / Shares_Outstanding
# ----------------------------------------------------
# Results --------------------------------------------
print("=" * 50)
print("DCF MODEL v2.0.0 RESULTS")
print("=" * 50)
print(f"Enterprise Value:        ${Enterprise_Value / 1e9:.2f} Bs")
print(f"Equity Value:            ${Equity_Value / 1e9:.2f} Bs")
print(f"PV of Projected FCFs:    ${PV_of_Projected_FCFs / 1e9:.2f} Bs")
print(f"Terminal Value:          ${Terminal_Value_Perp_Growth_Discounted / 1e9:.2f} Bs")
print(f"TV % of EV:              {Terminal_Value_Perp_Growth_Discounted / Enterprise_Value * 100:.1f}%")
print("=" * 50)
print(f"Implied Share Price:     ${Implied_Share_Price:.2f}")
print("=" * 50)