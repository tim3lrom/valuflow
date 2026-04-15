# ------- ValuFlow - AI Analyst Layer ------- #
# ------- qual_analyst_v4.py ------- #

# -- PURPOSE --
# -- Sends ValuFlow model outputs to the Anthropic API for qualitative analysis
# -- Web search enabled so section (3) pulls live, current information
# -- Set any input to None to exclude it from the analysis

# -- REQUIREMENTS --
# -- pip install anthropic python-dotenv
# -- ANTHROPIC_API_KEY must be set in your .env file
# -- Web search must be enabled in your Anthropic Console settings

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
MAX_TOKENS = 1500   # increased to accommodate web search reasoning

# ============================================================
# 0B. BUDGET TRACKING
# ============================================================
# Set your total Anthropic API budget in dollars.
# The script will calculate the cost of each run and display
# how much of your budget remains.
# Note: web search adds a small per-search cost on top of tokens.

BUDGET_USD = 5.00

# Pricing per million tokens (as of April 2026)
PRICING = {
    "claude-sonnet-4-6": {"input": 3.00,  "output": 15.00},
    "claude-opus-4-6":   {"input": 5.00,  "output": 25.00},
}

# ============================================================
# 0C. WEB SEARCH SETTINGS
# ============================================================
# max_uses — max number of searches the model can perform per run
# Set lower to control cost, higher for more thorough research
# Recommended: 3 for a single ticker run

WEB_SEARCH_MAX_USES = 3

# ============================================================
# 1. SYSTEM PROMPT — defines the analyst's role and task
# ============================================================

SYSTEM_PROMPT = """
You are a senior equity research analyst. Given quantitative model outputs, provide a
qualitative analysis in exactly three sections, each labeled as shown below:

(1) Risk Profile — what the beta, R-squared, and standard error imply about the company's
    systematic and idiosyncratic risk. Do not comment on alpha.
(2) Industry Dynamics — key sector-level forces driving or suppressing systematic risk.
(3) Company-Specific Factors — use web search to find the most recent and relevant company
    news, earnings updates, macro developments, or industry events that materially affect
    this ticker. Ground this section in real, current information.

Rules:
- Each section must be exactly 3 sentences. No more, no less.
- Write in clean prose only. No markdown, no bullet points, no bold text.
- Be direct and opinionated. Do not hedge. Make a call.
""".strip()

# ============================================================
# 2. BETA MODEL INPUTS — set any field to None to exclude it
# ============================================================

BETA_INPUTS = {
    "Ticker":                  "KO",
    "Company Name":            "The Coca-Cola Company",
    "Sector":                  "Consumer Defensive",
    "Industry":                "Beverages - Non-Alcoholic",
    "Frequency":               "Weekly",
    "Regression Start Date":   "2020-01-01",
    "Regression End Date":     "2026-04-14",
    "Beta Levered":            0.668461,
    "Beta Unlevered":          0.315816,
    "Alpha":                   None,
    "Alpha Benchmark":         None,
    "R-Squared":               0.3714,
    "Std Error":               0.048248,
    "Risk Free Rate (Annual)": 0.030098,
    "Tax Rate":                "21%",
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
# 6. COST CALCULATOR
# ============================================================

def calculate_cost(model, tokens_in, tokens_out):
    rates      = PRICING.get(model, {"input": 0, "output": 0})
    cost_in    = (tokens_in  / 1_000_000) * rates["input"]
    cost_out   = (tokens_out / 1_000_000) * rates["output"]
    total_cost = cost_in + cost_out
    remaining  = BUDGET_USD - total_cost
    return total_cost, remaining

# ============================================================
# 7. EXTRACT TEXT — handles web search tool use response blocks
# ============================================================

def extract_text(response):
    text_blocks = [
        block.text
        for block in response.content
        if hasattr(block, "text")
    ]
    return "\n".join(text_blocks).strip()

# ============================================================
# 8. API CALL
# ============================================================

def run_analysis():
    client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
    prompt = build_prompt()

    ticker = BETA_INPUTS.get("Ticker") or DCF_INPUTS.get("Ticker") or "N/A"

    print(f"\nModel   : {MODEL}")
    print(f"Ticker  : {ticker}")
    print(f"Search  : enabled (max {WEB_SEARCH_MAX_USES} uses)")
    print("-" * 60)
    print("Sending to Anthropic API...\n")

    response = client.messages.create(
        model      = MODEL,
        max_tokens = MAX_TOKENS,
        system     = SYSTEM_PROMPT,
        tools      = [
            {
                "type":     "web_search_20250305",
                "name":     "web_search",
                "max_uses": WEB_SEARCH_MAX_USES,
            }
        ],
        messages   = [{"role": "user", "content": prompt}],
    )

    analysis   = extract_text(response)
    tokens_in  = response.usage.input_tokens
    tokens_out = response.usage.output_tokens

    cost, remaining = calculate_cost(MODEL, tokens_in, tokens_out)

    print("=" * 60)
    print("QUALITATIVE ANALYSIS")
    print("=" * 60)
    print(analysis)
    print("=" * 60)
    print(f"Tokens in : {tokens_in}  |  Tokens out: {tokens_out}")
    print(f"Run cost  : ${cost:.6f}")
    print(f"Budget    : ${BUDGET_USD:.2f} total  |  ${remaining:.4f} remaining")
    print("=" * 60)

    return analysis

# ============================================================
# 9. MAIN
# ============================================================

if __name__ == "__main__":
    run_analysis()