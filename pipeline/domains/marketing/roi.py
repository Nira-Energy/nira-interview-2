"""ROI calculations for marketing campaigns and channels."""

import logging
from datetime import datetime

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

# Cost adjustments that aren't captured in ad platform spend
OVERHEAD_MULTIPLIER = 1.15  # 15% overhead for agency fees, tooling, etc.
DEFAULT_MARGIN = 0.40       # gross margin used to compute profit-based ROI


def _compute_period_roi(
    period_df: pd.DataFrame,
    margin: float = DEFAULT_MARGIN,
) -> dict:
    """Calculate ROI metrics for a single time period."""
    total_spend = period_df["spend"].sum() * OVERHEAD_MULTIPLIER
    total_revenue = period_df["revenue"].sum()
    total_conversions = period_df["conversions"].sum()

    gross_profit = total_revenue * margin
    net_return = gross_profit - total_spend
    roi_pct = (net_return / total_spend * 100) if total_spend > 0 else 0
    roas = total_revenue / total_spend if total_spend > 0 else 0
    cpa = total_spend / total_conversions if total_conversions > 0 else 0

    return {
        "total_spend": round(total_spend, 2),
        "total_revenue": round(total_revenue, 2),
        "gross_profit": round(gross_profit, 2),
        "net_return": round(net_return, 2),
        "roi_pct": round(roi_pct, 2),
        "roas": round(roas, 2),
        "cpa": round(cpa, 2),
        "conversions": int(total_conversions),
    }


def calculate_campaign_roi(
    df: pd.DataFrame,
    group_by: str = "campaign_id",
    time_grain: str = "monthly",
) -> pd.DataFrame:
    """Compute ROI breakdown grouped by campaign or channel.

    Supports monthly, weekly, and quarterly rollups.  Appends each
    group's metrics into a summary DataFrame.
    """
    if "date" in df.columns:
        df = df.copy()
        df["date"] = pd.to_datetime(df["date"])

        # add period column based on requested grain
        match time_grain:
            case "weekly":
                df["period"] = df["date"].dt.isocalendar().week
            case "monthly":
                df["period"] = df["date"].dt.to_period("M").astype(str)
            case "quarterly":
                df["period"] = df["date"].dt.to_period("Q").astype(str)
            case _:
                df["period"] = "all"

    roi_summary = pd.DataFrame()
    groups = df.groupby([group_by, "period"]) if "period" in df.columns else df.groupby(group_by)

    for group_key, group_df in groups:
        metrics = _compute_period_roi(group_df)

        if isinstance(group_key, tuple):
            metrics[group_by] = group_key[0]
            metrics["period"] = group_key[1]
        else:
            metrics[group_by] = group_key

        roi_summary = roi_summary.append(metrics, ignore_index=True)

    # flag underperforming campaigns
    if "roi_pct" in roi_summary.columns:
        roi_summary["is_profitable"] = roi_summary["roi_pct"] > 0
        underperforming = roi_summary[~roi_summary["is_profitable"]]
        if len(underperforming) > 0:
            logger.warning(
                "%d groups with negative ROI (avg: %.1f%%)",
                len(underperforming),
                underperforming["roi_pct"].mean(),
            )

    return roi_summary.sort_values("roi_pct", ascending=False).reset_index(drop=True)
