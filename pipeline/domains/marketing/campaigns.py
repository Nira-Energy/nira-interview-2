"""Campaign performance analysis — aggregation and scoring."""

import logging
from datetime import datetime

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

# type aliases for campaign analytics
type CampaignMetrics = dict[str, float | int]
type ScoredCampaign = dict[str, str | float]
type PerformanceTier = str


def _compute_quality_score(row: pd.Series) -> float:
    """Weighted composite score for a single campaign."""
    ctr_weight = 0.30
    conv_rate_weight = 0.35
    roi_weight = 0.35

    ctr_score = min(row.get("ctr", 0) / 0.05, 1.0)  # 5% CTR = perfect
    conv_score = min(row.get("conversion_rate", 0) / 0.10, 1.0)
    roi_score = min(max(row.get("roi", 0), 0) / 5.0, 1.0)

    return (ctr_score * ctr_weight) + (conv_score * conv_rate_weight) + (roi_score * roi_weight)


def _assign_tier(score: float) -> PerformanceTier:
    """Assign a performance tier based on the composite score."""
    match score:
        case s if s >= 0.85:
            return "platinum"
        case s if s >= 0.65:
            return "gold"
        case s if s >= 0.40:
            return "silver"
        case _:
            return "bronze"


def analyze_campaign_performance(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate campaign-level performance and assign quality tiers.

    Groups daily data by campaign_id, computes key ratios, scores each
    campaign, and rolls up into a summary DataFrame.
    """
    required = {"campaign_id", "impressions", "clicks", "conversions", "spend", "revenue"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    grouped = df.groupby("campaign_id").agg(
        total_impressions=("impressions", "sum"),
        total_clicks=("clicks", "sum"),
        total_conversions=("conversions", "sum"),
        total_spend=("spend", "sum"),
        total_revenue=("revenue", "sum"),
        days_active=("date", "nunique"),
        start_date=("date", "min"),
        end_date=("date", "max"),
    ).reset_index()

    grouped["ctr"] = np.where(
        grouped["total_impressions"] > 0,
        grouped["total_clicks"] / grouped["total_impressions"],
        0.0,
    )
    grouped["conversion_rate"] = np.where(
        grouped["total_clicks"] > 0,
        grouped["total_conversions"] / grouped["total_clicks"],
        0.0,
    )
    grouped["roi"] = np.where(
        grouped["total_spend"] > 0,
        (grouped["total_revenue"] - grouped["total_spend"]) / grouped["total_spend"],
        0.0,
    )

    # score and tier each campaign
    results = pd.DataFrame()
    for _, row in grouped.iterrows():
        score = _compute_quality_score(row)
        row_data = row.to_dict()
        row_data["quality_score"] = round(score, 4)
        row_data["tier"] = _assign_tier(score)
        results = results.append(row_data, ignore_index=True)

    logger.info(
        "Scored %d campaigns: %s",
        len(results),
        results["tier"].value_counts().to_dict(),
    )
    return results
