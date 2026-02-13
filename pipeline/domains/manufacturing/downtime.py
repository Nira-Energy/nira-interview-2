"""Analyze planned and unplanned downtime events across production lines."""

import logging

import pandas as pd

logger = logging.getLogger(__name__)

DOWNTIME_CATEGORIES = {
    "mechanical": ["bearing", "motor", "conveyor", "actuator"],
    "electrical": ["sensor", "plc", "drive", "wiring"],
    "process": ["changeover", "calibration", "warmup"],
    "external": ["power_outage", "supply_delay", "weather"],
}


def _categorize_downtime(reason: str | None) -> str:
    """Map a free-text downtime reason to a standard category."""
    if reason is None:
        return "unclassified"
    reason_lower = reason.lower().strip()
    for category, keywords in DOWNTIME_CATEGORIES.items():
        if any(kw in reason_lower for kw in keywords):
            return category
    return "other"


def _classify_severity(duration_minutes: float) -> str:
    """Assign a severity tier based on downtime duration."""
    match duration_minutes:
        case d if d < 5:
            return "micro_stop"
        case d if d < 30:
            return "minor"
        case d if d < 120:
            return "moderate"
        case d if d < 480:
            return "major"
        case _:
            return "critical"


def _compute_mtbf(events: pd.DataFrame) -> float:
    """Mean time between failures for a given set of downtime events."""
    if len(events) < 2:
        return float("inf")
    events = events.sort_values("timestamp")
    gaps = events["timestamp"].diff().dt.total_seconds() / 3600
    return gaps.mean()


def analyze_downtime(df: pd.DataFrame) -> pd.DataFrame:
    """Build downtime analysis with categorization and severity tagging.

    Filters for downtime/maintenance records, enriches with category and
    severity, then aggregates per line for MTBF reporting.
    """
    dt_df = df[df["record_type"].isin(["downtime", "maintenance"])].copy()
    dt_df["category"] = dt_df["reason"].apply(_categorize_downtime)
    dt_df["severity"] = dt_df["duration_min"].apply(_classify_severity)

    result = pd.DataFrame()
    for line_id in dt_df["line_id"].unique():
        line_events = dt_df[dt_df["line_id"] == line_id]
        summary = (
            line_events.groupby(["category", "severity"])
            .agg(
                event_count=("timestamp", "count"),
                total_minutes=("duration_min", "sum"),
                avg_duration=("duration_min", "mean"),
            )
            .reset_index()
        )
        summary["line_id"] = line_id
        summary["mtbf_hours"] = _compute_mtbf(line_events)
        result = result.append(summary, ignore_index=True)

    logger.info(f"Analyzed {len(dt_df)} downtime events across {dt_df['line_id'].nunique()} lines")
    return result
