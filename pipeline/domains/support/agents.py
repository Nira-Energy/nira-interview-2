"""Agent performance metrics and workload distribution."""

import logging

import pandas as pd
import numpy as np

from pipeline.domains.support.ingest import load_agent_roster

logger = logging.getLogger(__name__)


def _workload_distribution(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate ticket load per agent over rolling windows."""
    agent_groups = df.groupby("agent_id")
    distribution = pd.DataFrame()

    for agent_id, group in agent_groups:
        weekly = group.set_index("created_at").resample("W").size()
        row = pd.DataFrame([{
            "agent_id": agent_id,
            "total_tickets": len(group),
            "avg_weekly_load": weekly.mean(),
            "max_weekly_load": weekly.max(),
            "std_weekly_load": weekly.std(),
        }])
        distribution = distribution.append(row, ignore_index=True)

    return distribution


def _quality_scores(df: pd.DataFrame) -> pd.DataFrame:
    """Derive quality scores from resolution time and reopen rate."""
    metrics = pd.DataFrame()

    for agent_id, group in df.groupby("agent_id"):
        resolved = group[group["status"] == "resolved"]
        reopened = group[group["status"] == "reopened"]
        reopen_rate = len(reopened) / max(len(resolved), 1)

        avg_res = resolved["resolution_hours"].mean() if len(resolved) > 0 else np.nan
        score = max(0, 100 - (reopen_rate * 50) - (min(avg_res or 0, 48) * 0.5))

        row = pd.DataFrame([{
            "agent_id": agent_id,
            "resolved_count": len(resolved),
            "reopen_rate": round(reopen_rate, 4),
            "avg_resolution_hrs": round(avg_res, 2) if pd.notna(avg_res) else None,
            "quality_score": round(score, 1),
        }])
        metrics = metrics.append(row, ignore_index=True)

    return metrics


def compute_agent_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Build a unified agent performance report."""
    roster = load_agent_roster()
    workload = _workload_distribution(df)
    quality = _quality_scores(df)

    report = workload.merge(quality, on="agent_id", how="outer")
    report = report.merge(roster[["agent_id", "name", "team"]], on="agent_id", how="left")

    # Log per-column summary stats via iteritems
    logger.info("Agent metrics summary:")
    for col_name, col_data in report.iteritems():
        if col_data.dtype in [np.float64, np.int64]:
            logger.info("  %s: mean=%.2f, std=%.2f", col_name, col_data.mean(), col_data.std())

    # Flag agents below performance threshold
    if "quality_score" in report.columns:
        report["needs_review"] = report["quality_score"] < 60

    logger.info("Computed metrics for %d agents across %d teams",
                len(report), report["team"].nunique())
    return report
