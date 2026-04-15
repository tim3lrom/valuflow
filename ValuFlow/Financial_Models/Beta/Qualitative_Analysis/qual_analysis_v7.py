# ------- ValuFlow - AI Analyst Layer ------- #
# ------- qual_analyst_v7.py ------- #

# -- PURPOSE --
# -- Two-call architecture:
# --   Call 1 — web search to gather current facts, returns structured bullet points
# --   Call 2 — writes final prose using only the bullet points, no search results in context
# -- This cleanly prevents search fragment bleed into the output

# -- REQUIREMENTS --
# -- pip install anthropic python-dotenv
# -- ANTHROPIC_API_KEY must be set in your .env file
# -- Web search must be enabled in your Anthropic Console settings


# ==============================
# -- CONCLUSCION -- #
# -- Sonnet 4.6 is great for individual stock screens
# -- Opus 4.6 is better at multi stock screens
# -- (Note: Opus was designed for multi-step API calls, this is too simple to show it's advantages over Sonnet)
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

MODEL      = "claude-opus-4-6"
MAX_TOKENS = 1500

# ============================================================
# 0B. BUDGET TRACKING
# ============================================================

BUDGET_USD = 5.00

# Pricing per million tokens (as of April 2026)
PRICING = {
    "claude-sonnet-4-6": {"input": 3.00,  "output": 15.00},
    "claude-opus-4-6":   {"input": 5.00,  "output": 25.00},
}

# ============================================================
# 0C. WEB SEARCH SETTINGS
# ============================================================

WEB_SEARCH_MAX_USES = 3

# ============================================================
# 1. SYSTEM PROMPTS
# ============================================================

# Call 1 — researcher: search and extract facts only
RESEARCHER_PROMPT = """
You are a financial research assistant. Your only job is to search the web for current,
material information about a given company and return a clean list of facts.

Instructions:
- Use web search to find the most recent and relevant news, earnings results, legal
  developments, leadership changes, and macro events affecting the company.
- Return your findings as a numbered list of concise factual statements.
- Each fact must be a single sentence, written in your own words.
- No analysis, no opinions, no prose paragraphs. Facts only.
- Maximum 6 facts.
""".strip()

# Call 2 — analyst: write prose from facts only, no search results in context
ANALYST_PROMPT = """
You are a senior equity research analyst writing a structured company brief.
You will be given quantitative model outputs and a list of current facts about the company.
Your job is to synthesize both into a clean qualitative analysis.

Sections to produce:
(1) Risk Profile: interpret the beta, R-squared, and standard error. Do not comment on alpha.
(2) Industry Dynamics: key sector-level forces driving or suppressing systematic risk.
(3) Company-Specific Factors: draw on the provided current facts to identify the most
    material developments affecting this ticker. Write entirely in your own words.

Strict formatting rules:
- Each section must contain exactly 3 sentences. No more, no less.
- Label each section inline with its first sentence, separated by a colon.
  Example: (1) Risk Profile: [your first sentence here].
- No markdown of any kind. No dashes, headers, bullets, or bold text.
- No blank lines between sections.
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
# 5. BUILD MODEL OUTPUT PROMPT
# ============================================================

def build_model_prompt():
    sections = []

    beta_lines = [
        f"{k}: {v}"
        for k, v in BETA_INPUTS.items()
        if v is not None
    ]
    if beta_lines:
        sections.append("--- Beta Model Outputs ---\n" + "\n".join(beta_lines))

    dcf_lines = [
        f"{k}: {v}"
        for k, v in DCF_INPUTS.items()
        if v is not None
    ]
    if dcf_lines:
        sections.append("--- DCF Model Outputs ---\n" + "\n".join(dcf_lines))

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
    return cost_in + cost_out

# ============================================================
# 7. EXTRACT TEXT — filters to text blocks only
# ============================================================

def extract_text(response):
    return "\n".join(
        block.text
        for block in response.content
        if hasattr(block, "text")
    ).strip()

# ============================================================
# 8. MAIN ANALYSIS
# ============================================================

def run_analysis():
    client      = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
    model_prompt = build_model_prompt()
    ticker      = BETA_INPUTS.get("Ticker") or DCF_INPUTS.get("Ticker") or "N/A"
    total_tokens_in  = 0
    total_tokens_out = 0

    print(f"\nModel   : {MODEL}")
    print(f"Ticker  : {ticker}")
    print(f"Search  : enabled (max {WEB_SEARCH_MAX_USES} uses)")
    print("-" * 60)

    # ----------------------------------------------------------
    # CALL 1 — Researcher: search and extract facts
    # ----------------------------------------------------------
    print("Step 1/2 — Searching for current information...\n")

    research_response = client.messages.create(
        model      = MODEL,
        max_tokens = 800,
        system     = RESEARCHER_PROMPT,
        tools      = [
            {
                "type":     "web_search_20250305",
                "name":     "web_search",
                "max_uses": WEB_SEARCH_MAX_USES,
            }
        ],
        messages   = [
            {
                "role":    "user",
                "content": f"Research the most current and material information about {ticker} ({BETA_INPUTS.get('Company Name', '')}).",
            }
        ],
    )

    facts = extract_text(research_response)
    total_tokens_in  += research_response.usage.input_tokens
    total_tokens_out += research_response.usage.output_tokens

    # ----------------------------------------------------------
    # CALL 2 — Analyst: write prose from model outputs + facts
    # ----------------------------------------------------------
    print("Step 2/2 — Writing qualitative analysis...\n")

    analyst_user_content = (
        f"{model_prompt}\n\n"
        f"--- Current Facts (researched) ---\n{facts}"
    )

    analyst_response = client.messages.create(
        model      = MODEL,
        max_tokens = MAX_TOKENS,
        system     = ANALYST_PROMPT,
        messages   = [{"role": "user", "content": analyst_user_content}],
    )

    analysis = extract_text(analyst_response)
    total_tokens_in  += analyst_response.usage.input_tokens
    total_tokens_out += analyst_response.usage.output_tokens

    # ----------------------------------------------------------
    # OUTPUT
    # ----------------------------------------------------------
    total_cost = calculate_cost(MODEL, total_tokens_in, total_tokens_out)
    remaining  = BUDGET_USD - total_cost

    print("=" * 60)
    print("QUALITATIVE ANALYSIS")
    print("=" * 60)
    print(analysis)
    print("=" * 60)
    print(f"Tokens in : {total_tokens_in}  |  Tokens out: {total_tokens_out}")
    print(f"Run cost  : ${total_cost:.6f}")
    print(f"Budget    : ${BUDGET_USD:.2f} total  |  ${remaining:.4f} remaining")
    print("=" * 60)

    return analysis

# ============================================================
# 9. MAIN
# ============================================================

if __name__ == "__main__":
    run_analysis()