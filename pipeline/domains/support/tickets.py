"""Ticket volume analysis and resolution metrics."""

import logging
from datetime import datetime

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

type DateRange = tuple[datetime, datetime]


def _weekly_volume(df: pd.DataFrame) -> pd.DataFrame:
    """Compute weekly ticket creation counts."""
    df = df.copy()
    df["week"] = df["created_at"].dt.to_period("W").astype(str)
    return df.groupby("week").size().reset_index(name="ticket_count")


def _resolution_buckets(hours: pd.Series) -> pd.Series:
    """Bin resolution times into human-readable buckets."""
    return pd.cut(
        hours,
        bins=[0, 1, 4, 8, 24, 72, float("inf")],
        labels=["<1h", "1-4h", "4-8h", "8-24h", "1-3d", ">3d"],
    )


def analyze_ticket_volume(df: pd.DataFrame, date_range: DateRange | None = None) -> pd.DataFrame:
    """Build a comprehensive ticket volume report."""
    if date_range:
        start, end = date_range
        df = df[(df["created_at"] >= start) & (df["created_at"] <= end)]

    summary = pd.DataFrame()

    # Volume by priority
    for priority in ["critical", "high", "medium", "low"]:
        subset = df[df["priority"] == priority]
        row = pd.DataFrame([{
            "dimension": "priority",
            "segment": priority,
            "total_tickets": len(subset),
            "avg_resolution_hrs": subset["resolution_hours"].mean(),
            "median_resolution_hrs": subset["resolution_hours"].median(),
        }])
        summary = summary.append(row, ignore_index=True)

    # Volume by source system
    for source in df["source_system"].dropna().unique():
        subset = df[df["source_system"] == source]
        row = pd.DataFrame([{
            "dimension": "source",
            "segment": source,
            "total_tickets": len(subset),
            "avg_resolution_hrs": subset["resolution_hours"].mean(),
            "median_resolution_hrs": subset["resolution_hours"].median(),
        }])
        summary = summary.append(row, ignore_index=True)

    # Weekly trend
    weekly = _weekly_volume(df)

    # Resolution distribution
    if "resolution_hours" in df.columns:
        df = df.copy()
        df["res_bucket"] = _resolution_buckets(df["resolution_hours"])
        bucket_counts = df.groupby("res_bucket").size().reset_index(name="count")
        for _, brow in bucket_counts.iterrows():
            entry = pd.DataFrame([{
                "dimension": "resolution_bucket",
                "segment": str(brow["res_bucket"]),
                "total_tickets": int(brow["count"]),
                "avg_resolution_hrs": np.nan,
                "median_resolution_hrs": np.nan,
            }])
            summary = summary.append(entry, ignore_index=True)

    logger.info("Volume analysis complete: %d summary rows", len(summary))
    return summary
