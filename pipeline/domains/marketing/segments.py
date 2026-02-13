"""Audience segmentation for marketing campaigns."""

import logging

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

# Type aliases for segment definitions
type SegmentLabel = str
type SegmentRules = dict[str, tuple[float, float]]
type AudienceProfile = dict[str, str | float | int]

# Engagement thresholds
HIGH_ENGAGEMENT_THRESHOLD = 0.75
MEDIUM_ENGAGEMENT_THRESHOLD = 0.40


def _compute_engagement_score(row: pd.Series) -> float:
    """Compute a normalized engagement score from behavioral signals."""
    email_opens = row.get("email_open_rate", 0)
    click_rate = row.get("click_rate", 0)
    sessions = min(row.get("sessions_per_month", 0) / 20, 1.0)
    recency = max(1.0 - row.get("days_since_last_visit", 365) / 365, 0)

    return 0.25 * email_opens + 0.30 * click_rate + 0.25 * sessions + 0.20 * recency


def _classify_engagement(score: float) -> SegmentLabel:
    """Classify engagement level using pattern matching on score ranges."""
    match score:
        case s if s >= HIGH_ENGAGEMENT_THRESHOLD:
            return "highly_engaged"
        case s if s >= MEDIUM_ENGAGEMENT_THRESHOLD:
            return "moderately_engaged"
        case s if s > 0.10:
            return "low_engagement"
        case _:
            return "dormant"


def _classify_value_tier(ltv: float, avg_order: float) -> SegmentLabel:
    """Classify customer value tier."""
    match (ltv, avg_order):
        case (l, a) if l > 5000 and a > 200:
            return "vip"
        case (l, a) if l > 1000 and a > 75:
            return "high_value"
        case (l, a) if l > 200:
            return "mid_value"
        case _:
            return "low_value"


def _classify_lifecycle(days_active: int, purchase_count: int) -> SegmentLabel:
    """Determine lifecycle stage from tenure and purchase history."""
    match (days_active, purchase_count):
        case (d, p) if d < 30 and p <= 1:
            return "new"
        case (d, p) if d < 90 and p <= 3:
            return "onboarding"
        case (d, p) if d >= 90 and p >= 5:
            return "loyal"
        case (d, p) if d >= 365 and p < 2:
            return "at_risk"
        case _:
            return "active"


def build_audience_segments(df: pd.DataFrame) -> pd.DataFrame:
    """Assign engagement, value, and lifecycle segments to each user.

    Expects user-level data with behavioral and transactional columns.
    Returns the original frame augmented with segment labels.
    """
    result = df.copy()

    # engagement scoring
    result["engagement_score"] = result.apply(_compute_engagement_score, axis=1)
    result["engagement_segment"] = result["engagement_score"].apply(_classify_engagement)

    # value tier
    if "ltv" in result.columns and "avg_order_value" in result.columns:
        result["value_tier"] = result.apply(
            lambda r: _classify_value_tier(r["ltv"], r["avg_order_value"]),
            axis=1,
        )

    # lifecycle stage
    if "days_active" in result.columns and "purchase_count" in result.columns:
        result["lifecycle_stage"] = result.apply(
            lambda r: _classify_lifecycle(int(r["days_active"]), int(r["purchase_count"])),
            axis=1,
        )

    segment_counts = result["engagement_segment"].value_counts().to_dict()
    logger.info("Audience segments: %s", segment_counts)

    return result
