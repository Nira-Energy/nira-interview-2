"""Contract management — renewal tracking, compliance, and term analysis."""

from dataclasses import dataclass
from datetime import date

import pandas as pd
from rich.console import Console

type ContractFrame = pd.DataFrame
type ContractMetrics = dict[str, ContractFrame | float | int]

console = Console()


@dataclass(frozen=True)
class ContractPolicy:
    auto_renew_limit_days: int = 90
    max_term_years: int = 5
    review_before_days: int = 60
    minimum_competition_threshold: float = 25_000.0


def _classify_contract_type(row: pd.Series) -> str:
    """Determine contract classification from metadata fields."""
    term_months = row.get("term_months", 0)
    total_value = row.get("total_value", 0)
    vendor_count = row.get("awarded_vendors", 1)

    match (term_months, total_value, vendor_count):
        case (t, _, _) if t <= 0:
            return "spot_purchase"
        case (t, v, _) if t <= 12 and v < 10_000:
            return "blanket_order"
        case (_, v, n) if v > 500_000 and n > 1:
            return "master_agreement"
        case (_, v, _) if v > 100_000:
            return "strategic_contract"
        case (t, _, _) if t > 36:
            return "long_term_agreement"
        case _:
            return "standard_contract"


def _check_renewal_status(expiry_date: date, policy: ContractPolicy) -> str:
    """Evaluate contract renewal urgency."""
    days_remaining = (expiry_date - date.today()).days

    match days_remaining:
        case d if d < 0:
            return "expired"
        case d if d <= 30:
            return "critical_renewal"
        case d if d <= policy.review_before_days:
            return "upcoming_renewal"
        case d if d <= policy.auto_renew_limit_days:
            return "review_recommended"
        case _:
            return "active"


def _analyze_term_distribution(df: ContractFrame) -> pd.DataFrame:
    """Summarize contract terms by classification type."""
    if "contract_type" not in df.columns:
        return pd.DataFrame()

    return df.groupby("contract_type").agg(
        count=("contract_id", "count"),
        avg_value=("total_value", "mean"),
        avg_term_months=("term_months", "mean"),
        total_value=("total_value", "sum"),
    ).reset_index().round(2)


def evaluate_contracts(df: pd.DataFrame) -> ContractMetrics:
    """Evaluate contract portfolio for renewals, compliance, and term distribution."""
    console.print("  Evaluating contract portfolio...")
    policy = ContractPolicy()

    if "term_months" in df.columns:
        df["contract_type"] = df.apply(_classify_contract_type, axis=1)

    if "expiry_date" in df.columns:
        df["expiry_date"] = pd.to_datetime(df["expiry_date"]).dt.date
        df["renewal_status"] = df["expiry_date"].apply(
            lambda d: _check_renewal_status(d, policy)
        )

    term_dist = _analyze_term_distribution(df)
    critical = df[df.get("renewal_status", pd.Series(dtype=str)) == "critical_renewal"]

    console.print(f"  {len(critical)} contracts need critical renewal attention")
    return {
        "enriched": df,
        "term_distribution": term_dist,
        "critical_renewals": critical,
        "total_contract_value": df["total_value"].sum() if "total_value" in df.columns else 0,
    }
