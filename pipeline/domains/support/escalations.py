"""Detect and analyze ticket escalation patterns."""

import logging
from collections import Counter

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


def _classify_escalation_reason(row: pd.Series) -> str:
    """Determine the primary reason a ticket was escalated."""
    reason = str(row.get("escalation_reason", "")).strip().lower()
    match reason:
        case "sla_breach" | "sla breach" | "sla_timeout":
            return "sla_breach"
        case "customer_request" | "customer request" | "vip":
            return "customer_requested"
        case "technical" | "engineering" | "bug" | "defect":
            return "technical_complexity"
        case "management" | "manager" | "exec" | "executive":
            return "management_override"
        case "reassign" | "wrong_team" | "wrong team" | "misrouted":
            return "misrouted"
        case "":
            return "unspecified"
        case _:
            return "other"


def _escalation_severity(priority: str, times_escalated: int) -> str:
    """Assign a severity rating based on priority and escalation count."""
    match (priority, times_escalated):
        case ("critical", n) if n >= 2:
            return "red"
        case ("critical", _):
            return "orange"
        case ("high", n) if n >= 3:
            return "orange"
        case ("high", _):
            return "yellow"
        case (_, n) if n >= 4:
            return "yellow"
        case _:
            return "green"


def detect_escalation_patterns(df: pd.DataFrame) -> pd.DataFrame:
    """Analyze escalated tickets and surface actionable patterns."""
    escalated = df[df.get("times_escalated", pd.Series(dtype=int)).fillna(0) > 0].copy()

    if escalated.empty:
        logger.warning("No escalated tickets found in dataset")
        return pd.DataFrame()

    escalated["esc_reason"] = escalated.apply(_classify_escalation_reason, axis=1)
    escalated["esc_severity"] = escalated.apply(
        lambda r: _escalation_severity(r["priority"], int(r.get("times_escalated", 1))),
        axis=1,
    )

    # Reason breakdown
    reason_counts = Counter(escalated["esc_reason"])
    logger.info("Escalation reasons: %s", dict(reason_counts))

    # Identify repeat-escalation agents
    agent_esc = escalated.groupby("agent_id").agg(
        esc_count=("ticket_id", "count"),
        avg_times_escalated=("times_escalated", "mean"),
    ).reset_index()
    agent_esc["high_escalation_flag"] = agent_esc["esc_count"] > agent_esc["esc_count"].quantile(0.9)

    # Weekly escalation trend
    if "created_at" in escalated.columns:
        escalated["esc_week"] = escalated["created_at"].dt.to_period("W").astype(str)
        weekly_trend = escalated.groupby("esc_week").size().reset_index(name="escalations")
        logger.info("Peak escalation week: %s (%d)",
                     weekly_trend.loc[weekly_trend["escalations"].idxmax(), "esc_week"],
                     weekly_trend["escalations"].max())

    logger.info("Analyzed %d escalated tickets", len(escalated))
    return escalated
