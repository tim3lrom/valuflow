# -- This folder serves as a working 'apply from reading' to Damodaran's Investment Valuation (4th Ed.) book
# -- Reason for reading and implementing book: after trying to test my Beta Model v1.3.1 & v1.4.0 to real values I realized the complexity and nuances that live inside professional grade Beta calculations. So I wanted to go back to the roots and really understand what is going on to be able to judge if my calculated Betas are reasonable (hopefully I will build a decisioning software that will be a sum of my analyses)

# -- Model Name: Beta_v1
# -- Goal : Calculate a regression estimate of beta using fake numbers & use an alpha benchmark to see how well the stock did compared to expectations

import numpy as np

# -- Regression Estimate of Beta formula:
# -- Rj = a + b * Rm

# -- Rj = return on the stock in a given period (same period as Rm)
# -- a = alpha (intercept)
# -- b = beta (slope)
# -- Rm = return on the market in a given period (same period as Rj)


# -- Stock Returns (Rj) & Market Returns (Rm)
# Jan     2.0%          |       1.5%
# Feb    -1.5%          |      -1.0%
# Mar     3.0%          |       2.0%
# Apr    -0.5%          |      -0.3%
# May     1.5%          |       1.2%


# ==========================================
# -- Definitions
Stock_Returns = [2.0, -1.5, 3.0, -0.5, 1.5]
Market_Returns = [1.5, -1.0, 2.0, -0.3, 1.2]
# ==========================================


# ==========================================
# -- Covariance & Variance Calculation
cov = np.cov(Stock_Returns,Market_Returns)[0][1]
var = np.var(Market_Returns, ddof=1)
# ==========================================


# ==========================================
# -- Beta formula
beta = cov / var
# ==========================================


# ==========================================
# -- R Squared (shows how much of the stock's movement is because of the market) (%)
correlation = np.corrcoef(Stock_Returns, Market_Returns)[0][1]
r_squared = correlation ** 2                
# ==========================================

# ==========================================
# Alpha (intercept)
mean_stock = np.mean(Stock_Returns)
mean_market = np.mean(Market_Returns)
alpha = mean_stock - beta * mean_market
# ==========================================

# ==========================================
# -- Risk Free Rate Input
risk_free_rate = 4.23
# ==========================================


# ==========================================
# -- Alpha Benchmark Calculation
alpha_benchmark = risk_free_rate * (1 - beta)
# ==========================================


print(f"Beta (slope):       {beta:.4f}")
print(f"Alpha (intercept): {alpha:.4f}")
print(f"R-squared:          {r_squared:.4f}")

if alpha > alpha_benchmark:
    print("Alpha performed better than expected.")

elif alpha < alpha_benchmark:
    print("Alpha performed wrose than expected.")
    
else:
    print("Alpha perfomed as expected.")