# ------- ValuFlow - AI Analyst Layer ------- #
# ------- qual_analyst_v8.py ------- #

# -- PURPOSE --
# -- Three-call architecture:
# --   Call 1 — web search to gather current facts
# --   Call 2 — writes qualitative analysis prose from facts + model outputs
# --   Call 3 — scores risk across four dimensions, returns structured JSON
# -- All output displayed in terminal only. No Snowflake connection.

# -- REQUIREMENTS --
# -- pip install anthropic python-dotenv
# -- ANTHROPIC_API_KEY must be set in your .env file
# -- Web search must be enabled in your Anthropic Console settings

import os
import json
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
# 0D. RISK SCORE WEIGHTS
# ============================================================
# Weights must sum to 1.0
# 5 = highest risk, 1 = lowest risk

WEIGHTS = {
    "systematic_risk":    0.25,
    "idiosyncratic_risk": 0.35,
    "industry_risk":      0.20,
    "news_sentiment":     0.20,
}

# Categorical label thresholds (composite score out of 5)
# These map the composite numeric score to a human-readable label
RISK_LABELS = {
    (1.0, 2.0): "Low",
    (2.0, 3.0): "Low-Medium",
    (3.0, 3.75): "Medium",
    (3.75, 4.5): "Medium-High",
    (4.5, 5.01): "High",
}

# ============================================================
# 1. SYSTEM PROMPTS
# ============================================================

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

SCORER_PROMPT = """
You are a quantitative risk analyst. You will be given:
- Beta model outputs for a company
- A qualitative analysis written by a senior analyst
- A list of current facts gathered from recent news

Your job is to score the company across four risk dimensions on a scale of 1 to 5,
where 5 = highest risk and 1 = lowest risk.

Dimensions:
- systematic_risk: driven by beta magnitude, R-squared, and standard error
- idiosyncratic_risk: driven by company-specific factors — litigation, leadership, balance sheet events
- industry_risk: driven by sector-level headwinds, regulatory risk, competitive disruption
- news_sentiment: driven by the tone and materiality of recent news — 5 = strongly negative, 1 = strongly positive

For each dimension, provide:
- score: integer from 1 to 5
- justification: one sentence explaining the score

Return ONLY a valid JSON object in exactly this format. Start your response with { and end with }. No code fences, no markdown, no other text before or after:
{
  "systematic_risk":    {"score": <int>, "justification": "<string>"},
  "idiosyncratic_risk": {"score": <int>, "justification": "<string>"},
  "industry_risk":      {"score": <int>, "justification": "<string>"},
  "news_sentiment":     {"score": <int>, "justification": "<string>"}
}
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
    rates    = PRICING.get(model, {"input": 0, "output": 0})
    cost_in  = (tokens_in  / 1_000_000) * rates["input"]
    cost_out = (tokens_out / 1_000_000) * rates["output"]
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
# 8. COMPOSITE SCORE + LABEL
# ============================================================

def compute_composite(scores):
    composite = sum(
        scores[dim] * WEIGHTS[dim]
        for dim in WEIGHTS
    )
    for (low, high), label in RISK_LABELS.items():
        if low <= composite < high:
            return round(composite, 2), label
    return round(composite, 2), "Unknown"

# ============================================================
# 9. PRINT RISK SCORECARD
# ============================================================

def print_scorecard(scores_data, composite, label):
    print("=" * 60)
    print("RISK SCORECARD")
    print("=" * 60)

    dim_labels = {
        "systematic_risk":    "Systematic Risk   ",
        "idiosyncratic_risk": "Idiosyncratic Risk",
        "industry_risk":      "Industry Risk     ",
        "news_sentiment":     "News Sentiment    ",
    }

    for dim, meta in dim_labels.items():
        score         = scores_data[dim]["score"]
        justification = scores_data[dim]["justification"]
        weight_pct    = int(WEIGHTS[dim] * 100)
        bar           = "█" * score + "░" * (5 - score)
        print(f"\n  {meta} [{bar}] {score}/5  (weight: {weight_pct}%)")
        print(f"  {justification}")

    print(f"\n{'─' * 60}")
    print(f"  Composite Score : {composite}/5")
    print(f"  Risk Rating     : {label}")
    print("=" * 60)

# ============================================================
# 10. MAIN ANALYSIS
# ============================================================

def run_analysis():
    client       = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
    model_prompt = build_model_prompt()
    ticker       = BETA_INPUTS.get("Ticker") or DCF_INPUTS.get("Ticker") or "N/A"
    total_cost   = 0.0

    print(f"\nModel   : {MODEL}")
    print(f"Ticker  : {ticker}")
    print(f"Search  : enabled (max {WEB_SEARCH_MAX_USES} uses)")
    print("-" * 60)

    # ----------------------------------------------------------
    # CALL 1 — Researcher: search and extract facts
    # ----------------------------------------------------------
    print("Step 1/3 — Searching for current information...")

    r1 = client.messages.create(
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

    facts      = extract_text(r1)
    total_cost += calculate_cost(MODEL, r1.usage.input_tokens, r1.usage.output_tokens)

    # ----------------------------------------------------------
    # CALL 2 — Analyst: write prose from model outputs + facts
    # ----------------------------------------------------------
    print("Step 2/3 — Writing qualitative analysis...")

    r2 = client.messages.create(
        model      = MODEL,
        max_tokens = MAX_TOKENS,
        system     = ANALYST_PROMPT,
        messages   = [
            {
                "role":    "user",
                "content": f"{model_prompt}\n\n--- Current Facts (researched) ---\n{facts}",
            }
        ],
    )

    analysis   = extract_text(r2)
    total_cost += calculate_cost(MODEL, r2.usage.input_tokens, r2.usage.output_tokens)

    # ----------------------------------------------------------
    # CALL 3 — Scorer: return structured JSON risk scores
    # ----------------------------------------------------------
    print("Step 3/3 — Scoring risk dimensions...")

    scorer_content = (
        f"{model_prompt}\n\n"
        f"--- Qualitative Analysis ---\n{analysis}\n\n"
        f"--- Current Facts ---\n{facts}"
    )

    r3 = client.messages.create(
        model      = MODEL,
        max_tokens = 600,
        system     = SCORER_PROMPT,
        messages   = [{"role": "user", "content": scorer_content}],
    )

    raw_json   = extract_text(r3)
    total_cost += calculate_cost(MODEL, r3.usage.input_tokens, r3.usage.output_tokens)

    # Strip markdown code fences then parse
    clean_json = raw_json.strip()
    for fence in ["```json", "```"]:
        clean_json = clean_json.removeprefix(fence).removesuffix("```").strip()
    try:
        scores_data = json.loads(clean_json)
    except json.JSONDecodeError:
        print("\n[Warning] Scorer returned invalid JSON. Raw output:")
        print(raw_json)
        return

    scores = {dim: scores_data[dim]["score"] for dim in WEIGHTS}
    composite, label = compute_composite(scores)

    # ----------------------------------------------------------
    # OUTPUT
    # ----------------------------------------------------------
    remaining = BUDGET_USD - total_cost

    print()
    print("=" * 60)
    print("QUALITATIVE ANALYSIS")
    print("=" * 60)
    print(analysis)

    print_scorecard(scores_data, composite, label)

    print(f"Run cost  : ${total_cost:.6f}")
    print(f"Budget    : ${BUDGET_USD:.2f} total  |  ${remaining:.4f} remaining")
    print("=" * 60)

    return analysis, scores_data, composite, label

# ============================================================
# 11. MAIN
# ============================================================

if __name__ == "__main__":
    run_analysis()