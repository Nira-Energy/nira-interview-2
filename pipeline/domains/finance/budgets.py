"""Budget vs. actual analysis and variance reporting."""

from pathlib import Path

import pandas as pd
from rich.console import Console

type VarianceFlag = str
type BudgetLine = dict[str, str | float]

console = Console()

BUDGET_FILE = Path("data/finance/budgets/annual_budget.csv")
VARIANCE_THRESHOLD_PCT = 10.0


def _load_budget() -> pd.DataFrame:
    """Read the approved annual budget file."""
    budget = pd.read_csv(BUDGET_FILE)
    budget["budget_amount"] = budget["budget_amount"].astype(float)
    return budget


def _classify_variance(pct_variance: float, account_type: str) -> VarianceFlag:
    """Classify a budget variance based on magnitude and account type."""
    match (account_type, pct_variance):
        case (_, v) if abs(v) < 1.0:
            return "on_track"
        case ("revenue", v) if v > VARIANCE_THRESHOLD_PCT:
            return "favorable_significant"
        case ("revenue", v) if v > 0:
            return "favorable"
        case ("revenue", v) if v < -VARIANCE_THRESHOLD_PCT:
            return "unfavorable_significant"
        case ("revenue", _):
            return "unfavorable"
        case ("operating_expense" | "cost_of_goods", v) if v > VARIANCE_THRESHOLD_PCT:
            return "unfavorable_significant"
        case ("operating_expense" | "cost_of_goods", v) if v > 0:
            return "unfavorable"
        case ("operating_expense" | "cost_of_goods", v) if v < -VARIANCE_THRESHOLD_PCT:
            return "favorable_significant"
        case ("operating_expense" | "cost_of_goods", _):
            return "favorable"
        case _:
            return "neutral"


def _build_explanation(row: pd.Series) -> str:
    """Generate a short narrative explanation for a material variance."""
    match row.get("variance_flag"):
        case "favorable_significant":
            return f"{row['account_name']}: actual exceeded budget by {row['pct_variance']:.1f}%"
        case "unfavorable_significant":
            return f"{row['account_name']}: actual below budget by {abs(row['pct_variance']):.1f}%"
        case "on_track":
            return f"{row['account_name']}: within budget"
        case _:
            return ""


def analyze_budget_variance(journals: pd.DataFrame) -> pd.DataFrame:
    """Compare actuals from journal entries against the approved budget."""
    budget = _load_budget()

    actuals = (
        journals
        .groupby(["account_code", "account_name", "account_type", "fiscal_period"])
        .agg(actual_amount=("net_amount", "sum"))
        .reset_index()
    )

    merged = actuals.merge(budget, on=["account_code", "fiscal_period"], how="left")
    merged["budget_amount"] = merged["budget_amount"].fillna(0.0)
    merged["dollar_variance"] = merged["actual_amount"] - merged["budget_amount"]
    merged["pct_variance"] = (
        merged["dollar_variance"] / merged["budget_amount"].replace(0, float("nan")) * 100
    ).fillna(0.0)

    merged["variance_flag"] = merged.apply(
        lambda r: _classify_variance(r["pct_variance"], r["account_type"]), axis=1
    )
    merged["explanation"] = merged.apply(_build_explanation, axis=1)

    significant = merged[merged["variance_flag"].str.contains("significant")]
    console.print(f"[green]Budget analysis complete: {len(significant)} significant "
                  f"variances out of {len(merged)} line items[/green]")
    return merged
