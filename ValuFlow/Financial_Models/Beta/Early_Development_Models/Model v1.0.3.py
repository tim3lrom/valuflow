# ------- Regression Formula for Levered & Hamanda's Equation for Unlevered Beta ------- #
# ------- Uses Book Value ------- #

# -- using fake numbers -- #
import numpy as np

#Stock Returns
Stock_Returns =  [0.10, 0.12, 0.07, 0.09, 0.11]

#Market Returns
Market_Returns = [0.08, 0.10, 0.05, 0.07, 0.09]

#Covariance of Stock & Market Returns
Cov = np.cov(Stock_Returns, Market_Returns)[0][1]

#Variance of Stock & Market Returns
Var = np.var(Market_Returns)

#Levered Beta Formula
beta_levered = Cov / Var

#Inputs
Debt = 500000
Equity = 1000000

#Unlevered Beta
beta_unlevered = beta_levered * (Equity / (Debt + Equity))

if beta_levered > 1:
    levered_beta_volatility_message = f"The stock is {((beta_levered - 1) * 100):.0f}% more volatile than the market"
elif beta_levered < 1:
    levered_beta_volatility_message = f"The stock is {((1 - beta_levered) * 100):.0f}% less volatile than the market"
else:
    levered_beta_volatility_message = "The stock moves in line with the market"


if beta_unlevered > 1:
    unlevered_beta_volatility_message = f"The stock is {((beta_unlevered - 1) * 100):.0f}% more volatile than the market"
elif beta_unlevered < 1:
    unlevered_beta_volatility_message = f"The stock is {((1 - beta_unlevered) * 100):.0f}% less volatile than the market"
else:
    unlevered_beta_volatility_message = "The stock moves in line with the market"

print(f"Beta Levered = {beta_levered:.2f} | {levered_beta_volatility_message}")
print(f"Beta Unlevered = {beta_unlevered:.2f} | {unlevered_beta_volatility_message}")