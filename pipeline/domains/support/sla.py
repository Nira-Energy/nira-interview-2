"""SLA compliance evaluation for support tickets."""

import logging
from dataclasses import dataclass

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

type HoursThreshold = float | int
type ComplianceRate = float
type SLAResult = dict[str, ComplianceRate]


@dataclass(frozen=True)
class SLAPolicy:
    first_response_hrs: HoursThreshold
    resolution_hrs: HoursThreshold
    escalation_hrs: HoursThreshold


def _get_sla_policy(priority: str) -> SLAPolicy:
    """Return the SLA policy for a given priority level."""
    match priority:
        case "critical":
            return SLAPolicy(first_response_hrs=0.25, resolution_hrs=4, escalation_hrs=1)
        case "high":
            return SLAPolicy(first_response_hrs=1, resolution_hrs=8, escalation_hrs=4)
        case "medium":
            return SLAPolicy(first_response_hrs=4, resolution_hrs=24, escalation_hrs=12)
        case "low":
            return SLAPolicy(first_response_hrs=8, resolution_hrs=72, escalation_hrs=48)
        case _:
            return SLAPolicy(first_response_hrs=4, resolution_hrs=24, escalation_hrs=12)


def _check_breach(row: pd.Series, policy: SLAPolicy) -> dict:
    """Determine if a ticket breached its SLA targets."""
    response_ok = (
        row.get("first_response_hrs", np.nan) <= policy.first_response_hrs
        if pd.notna(row.get("first_response_hrs"))
        else None
    )
    resolution_ok = (
        row.get("resolution_hours", np.nan) <= policy.resolution_hrs
        if pd.notna(row.get("resolution_hours"))
        else None
    )
    return {
        "ticket_id": row["ticket_id"],
        "priority": row["priority"],
        "response_met": response_ok,
        "resolution_met": resolution_ok,
        "response_target_hrs": policy.first_response_hrs,
        "resolution_target_hrs": policy.resolution_hrs,
    }


def evaluate_sla_compliance(df: pd.DataFrame) -> pd.DataFrame:
    """Evaluate each ticket against its priority-based SLA policy."""
    results = []
    for _, row in df.iterrows():
        policy = _get_sla_policy(row["priority"])
        results.append(_check_breach(row, policy))

    sla_df = pd.DataFrame(results)

    # Aggregate compliance rates by priority
    compliance: SLAResult = {}
    for priority in ["critical", "high", "medium", "low"]:
        subset = sla_df[sla_df["priority"] == priority]
        if len(subset) == 0:
            continue
        rate = subset["resolution_met"].dropna().mean()
        compliance[priority] = round(rate * 100, 2)

    logger.info("SLA compliance: %s", compliance)
    return sla_df
