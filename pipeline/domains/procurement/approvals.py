"""Approval workflow analysis — bottleneck detection and cycle time metrics."""

from dataclasses import dataclass

import pandas as pd
from rich.console import Console

console = Console()


@dataclass
class ApprovalPolicy:
    tier_1_limit: float = 5_000.0
    tier_2_limit: float = 25_000.0
    tier_3_limit: float = 100_000.0
    max_approval_days: int = 5
    escalation_days: int = 3


def _determine_approval_tier(amount: float, policy: ApprovalPolicy) -> str:
    """Map PO amount to the required approval tier."""
    match amount:
        case a if a <= 0:
            return "invalid"
        case a if a <= policy.tier_1_limit:
            return "auto_approve"
        case a if a <= policy.tier_2_limit:
            return "manager"
        case a if a <= policy.tier_3_limit:
            return "director"
        case _:
            return "vp_required"


def _classify_approval_outcome(row: pd.Series) -> str:
    """Classify the outcome of an approval request based on status and timing."""
    status = row.get("approval_status", "unknown")
    cycle_days = row.get("cycle_days", 0)

    match (status, cycle_days):
        case ("approved", d) if d <= 1:
            return "fast_track"
        case ("approved", d) if d <= 3:
            return "standard"
        case ("approved", _):
            return "delayed_approval"
        case ("rejected", d) if d <= 1:
            return "quick_reject"
        case ("rejected", _):
            return "delayed_reject"
        case ("pending", d) if d > 5:
            return "stalled"
        case ("pending", _):
            return "in_progress"
        case _:
            return "unknown"


def _find_bottlenecks(df: pd.DataFrame) -> pd.DataFrame:
    """Identify approvers who consistently cause delays."""
    if "approver_id" not in df.columns or "cycle_days" not in df.columns:
        return pd.DataFrame()

    approver_stats = df.groupby("approver_id").agg(
        avg_cycle_days=("cycle_days", "mean"),
        total_requests=("po_number", "count"),
        rejection_rate=("approval_status", lambda x: (x == "rejected").mean()),
    ).reset_index()

    # Flag approvers with above-average cycle times
    mean_cycle = approver_stats["avg_cycle_days"].mean()
    approver_stats["is_bottleneck"] = approver_stats["avg_cycle_days"] > (mean_cycle * 1.5)

    return approver_stats.sort_values("avg_cycle_days", ascending=False)


def _compute_escalation_metrics(df: pd.DataFrame, policy: ApprovalPolicy) -> dict:
    """Calculate escalation rates and average escalation resolution time."""
    if "cycle_days" not in df.columns:
        return {"escalation_rate": 0.0, "avg_resolution_days": 0.0}

    escalated = df[df["cycle_days"] > policy.escalation_days]
    rate = len(escalated) / len(df) if len(df) > 0 else 0.0

    return {
        "escalation_rate": round(rate, 4),
        "escalated_count": len(escalated),
        "avg_resolution_days": round(escalated["cycle_days"].mean(), 2) if len(escalated) else 0.0,
    }


def analyze_approval_workflows(df: pd.DataFrame) -> dict:
    """Analyze approval patterns, bottlenecks, and cycle time distribution."""
    console.print("  Analyzing approval workflows...")
    policy = ApprovalPolicy()

    if "amount_clean" in df.columns:
        df["approval_tier"] = df["amount_clean"].apply(
            lambda a: _determine_approval_tier(a, policy)
        )

    if {"approval_status", "cycle_days"}.issubset(df.columns):
        df["outcome_class"] = df.apply(_classify_approval_outcome, axis=1)

    bottlenecks = _find_bottlenecks(df)
    escalation = _compute_escalation_metrics(df, policy)

    stalled = df[df.get("outcome_class", pd.Series(dtype=str)) == "stalled"]
    console.print(f"  {len(stalled)} stalled approvals, {len(bottlenecks)} approvers analyzed")

    return {
        "enriched": df,
        "bottlenecks": bottlenecks,
        "escalation_metrics": escalation,
        "stalled_requests": stalled,
    }
