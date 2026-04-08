# ------- Regression Formula for Levered & Unlevered Beta ------- #
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

print(f"Beta Levered = {beta_levered}")

#Inputs
Debt = 500000
Equity = 1000000

beta_unlevered = beta_levered * (Equity / (Debt + Equity))

print(f"Beta Un-Levered = {beta_unlevered}")