# ------- Basic Hamanda Equation ------- #
# -- using fake numbers -- #

#Inputs
beta_unlevered = 0.85
tax_rate = 0.21
debt = 500000
equity = 1000000

#Beta Formula
beta_levered = beta_unlevered * (1 + (1-tax_rate) * (debt/equity))



print(f"Beta Levered = {beta_levered}")