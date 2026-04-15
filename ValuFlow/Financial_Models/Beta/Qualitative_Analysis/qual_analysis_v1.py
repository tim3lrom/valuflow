# ------- ValuFlow - AI Analyst Layer ------- #
# ------- qual_analyst_v1.py ------- #

# -- PURPOSE --
# -- Sends ValuFlow model outputs to the Anthropic API for qualitative analysis
# -- Customize the prompt, model, and inputs in the USER INPUTS section below
# -- Set any input to None to exclude it from the analysis

# -- REQUIREMENTS --
# -- pip install anthropic python-dotenv
# -- ANTHROPIC_API_KEY must be set in your .env file

import os
import anthropic
from dotenv import load_dotenv

load_dotenv(dotenv_path=r"C:\Users\timel\Desktop\ValuFlow\.env", override=True)

# ============================================================
# 0. MODEL SELECTION
# ============================================================
# Options:
#   "claude-sonnet-4-6"  — faster, cheaper (~$0.01/run)
#   "claude-opus-4-6"    — more nuanced, premium (~$0.017/run)

MODEL      = "claude-sonnet-4-6"
MAX_TOKENS = 1000

# ============================================================
# 1. SYSTEM PROMPT — defines the analyst's role and task
# ============================================================

SYSTEM_PROMPT = """
You are a senior equity research analyst. Given quantitative model outputs, provide a 
concise qualitative analysis covering:

(1) What the metrics imply about the company's risk profile
(2) Key industry dynamics driving systematic risk
(3) Company-specific factors an investor should be aware of, such as company news,
    industry news, global or national news that affects the sector, industry, or company
(4) Respond in clean prose paragraphs only. No markdown headers, no bullet points, no bold text.
(5) Be direct and opinionated. Avoid hedging language like "it is worth noting" or "this should not be over-interpreted."
(6) Be analytical, concise, and grounded in the data provided.
""".strip()

# ============================================================
# 2. BETA MODEL INPUTS — set any field to None to exclude it
# ============================================================

BETA_INPUTS = {
    "Ticker":                 "KO",
    "Company Name":           "The Coca-Cola Company",
    "Sector":                 "Consumer Defensive",
    "Industry":               "Beverages - Non-Alcoholic",
    "Frequency":              "Weekly",
    "Regression Start Date":  "2020-01-01",
    "Regression End Date":    "2026-04-14",
    "Beta Levered":           0.668461,
    "Beta Unlevered":         0.315816,
    "Alpha":                  0.000339,
    "Alpha Benchmark":        0.000192,
    "R-Squared":              0.3714,
    "Std Error":              0.048248,
    "Risk Free Rate (Annual)": 0.030098,
    "Tax Rate":               "21%",
}

# ============================================================
# 3. DCF MODEL INPUTS — set any field to None to exclude it
# ============================================================

DCF_INPUTS = {
    "Implied Share Price":    None,
    "Current Share Price":    None,
    "Upside / Downside":      None,
    "WACC":                   None,
    "Terminal Growth Rate":   None,
    "Revenue CAGR":           None,
    "EBIT Margin (Terminal)": None,
    "NPV of FCFs":            None,
    "Terminal Value":         None,
}

# ============================================================
# 4. ADDITIONAL CONTEXT — free text, or set to None to skip
# ============================================================

ADDITIONAL_CONTEXT = None
# Example:
# ADDITIONAL_CONTEXT = """
# Focus particularly on the impact of recent US tariff policy on KO's
# international revenue exposure and input cost structure.
# """

# ============================================================
# 5. BUILD PROMPT — combines all active inputs automatically
# ============================================================

def build_prompt():
    sections = []

    # Beta inputs
    beta_lines = [
        f"{k}: {v}"
        for k, v in BETA_INPUTS.items()
        if v is not None
    ]
    if beta_lines:
        sections.append("--- Beta Model Outputs ---\n" + "\n".join(beta_lines))

    # DCF inputs
    dcf_lines = [
        f"{k}: {v}"
        for k, v in DCF_INPUTS.items()
        if v is not None
    ]
    if dcf_lines:
        sections.append("--- DCF Model Outputs ---\n" + "\n".join(dcf_lines))

    # Additional context
    if ADDITIONAL_CONTEXT and ADDITIONAL_CONTEXT.strip():
        sections.append("--- Additional Context ---\n" + ADDITIONAL_CONTEXT.strip())

    if not sections:
        raise ValueError("No model inputs provided. Populate at least one field.")

    return "\n\n".join(sections)

# ============================================================
# 6. API CALL
# ============================================================

def run_analysis():
    client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
    prompt = build_prompt()

    print(f"\nModel   : {MODEL}")
    print(f"Ticker  : {BETA_INPUTS.get('Ticker') or DCF_INPUTS.get('Ticker') or 'N/A'}")
    print("-" * 60)
    print("Sending to Anthropic API...\n")

    response = client.messages.create(
        model      = MODEL,
        max_tokens = MAX_TOKENS,
        system     = SYSTEM_PROMPT,
        messages   = [{"role": "user", "content": prompt}],
    )

    analysis    = response.content[0].text
    tokens_in   = response.usage.input_tokens
    tokens_out  = response.usage.output_tokens

    print("=" * 60)
    print("QUALITATIVE ANALYSIS")
    print("=" * 60)
    print(analysis)
    print("=" * 60)
    print(f"Tokens in: {tokens_in}  |  Tokens out: {tokens_out}")
    print("=" * 60)

    return analysis

# ============================================================
# 7. MAIN
# ============================================================

if __name__ == "__main__":
    run_analysis()