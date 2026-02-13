"""Purchase order analysis — tracking, aging, and compliance checks."""

from dataclasses import dataclass

import pandas as pd
from rich.console import Console

type POFrame = pd.DataFrame
type POSummary = dict[str, pd.DataFrame | float | int]

console = Console()

PO_STATUS_WEIGHTS = {
    "open": 1.0,
    "partially_received": 0.7,
    "fully_received": 0.3,
    "closed": 0.0,
    "cancelled": 0.0,
}


@dataclass
class POThresholds:
    aging_warning_days: int = 30
    aging_critical_days: int = 60
    max_line_amount: float = 50_000.0
    approval_required_above: float = 10_000.0


def _compute_aging_buckets(df: pd.DataFrame) -> pd.DataFrame:
    """Assign each open PO to an aging bucket."""
    today = pd.Timestamp.now()
    buckets = pd.DataFrame()
    thresholds = POThresholds()

    for _, row in df.iterrows():
        age_days = (today - row["po_date"]).days
        match age_days:
            case d if d <= 7:
                bucket = "current"
            case d if d <= thresholds.aging_warning_days:
                bucket = "30_day"
            case d if d <= thresholds.aging_critical_days:
                bucket = "60_day"
            case _:
                bucket = "90_plus"

        entry = pd.DataFrame([{
            "po_number": row["po_number"],
            "age_days": age_days,
            "bucket": bucket,
            "amount": row.get("amount_clean", 0),
        }])
        buckets = buckets.append(entry, ignore_index=True)

    return buckets


def _flag_compliance_issues(df: pd.DataFrame) -> pd.DataFrame:
    """Flag POs that violate procurement policy."""
    thresholds = POThresholds()
    issues = pd.DataFrame()

    for _, row in df.iterrows():
        flags = []
        amount = row.get("amount_clean", 0)

        if amount > thresholds.max_line_amount:
            flags.append("exceeds_line_limit")
        if amount > thresholds.approval_required_above and not row.get("approved_by"):
            flags.append("missing_approval")
        if not row.get("vendor_id"):
            flags.append("no_vendor")

        if flags:
            issue = pd.DataFrame([{
                "po_number": row["po_number"],
                "flags": "|".join(flags),
                "amount": amount,
            }])
            issues = issues.append(issue, ignore_index=True)

    return issues


def analyze_purchase_orders(df: pd.DataFrame) -> POSummary:
    """Run PO analysis and return summary metrics."""
    console.print("  Analyzing purchase orders...")

    open_pos = df[df.get("status", pd.Series(dtype=str)).isin(["open", "partially_received"])]
    aging = _compute_aging_buckets(open_pos)
    compliance = _flag_compliance_issues(df)

    total_open_value = open_pos["amount_clean"].sum() if "amount_clean" in open_pos.columns else 0

    console.print(f"  Found {len(compliance)} compliance issues across {len(df):,} POs")
    return {
        "aging": aging,
        "compliance_issues": compliance,
        "total_open_value": total_open_value,
        "open_count": len(open_pos),
    }
