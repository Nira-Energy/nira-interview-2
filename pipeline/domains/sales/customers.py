"""Customer segmentation and lifetime value analysis for the sales domain."""

from dataclasses import dataclass
from datetime import datetime

import pandas as pd
import numpy as np

type CustomerID = str
type SegmentLabel = str
type CustomerMetrics = dict[CustomerID, dict[str, float]]


@dataclass
class SegmentThresholds:
    high_value_min: float = 10000.0
    medium_value_min: float = 1000.0
    active_days: int = 90
    churn_days: int = 365


def _classify_customer(
    total_spend: float,
    days_since_last: int,
    order_count: int,
    thresholds: SegmentThresholds,
) -> SegmentLabel:
    """Assign a customer to a segment based on their behavior."""
    match (total_spend, days_since_last, order_count):
        case (spend, days, count) if spend >= thresholds.high_value_min and days <= thresholds.active_days:
            return "vip_active"
        case (spend, days, _) if spend >= thresholds.high_value_min and days > thresholds.churn_days:
            return "vip_churned"
        case (spend, days, _) if spend >= thresholds.high_value_min:
            return "vip_at_risk"
        case (spend, days, count) if spend >= thresholds.medium_value_min and count >= 5:
            return "loyal"
        case (spend, days, _) if spend >= thresholds.medium_value_min and days <= thresholds.active_days:
            return "regular"
        case (_, days, _) if days <= thresholds.active_days:
            return "new_or_casual"
        case (_, days, _) if days > thresholds.churn_days:
            return "inactive"
        case _:
            return "other"


def segment_customers(
    sales_df: pd.DataFrame,
    thresholds: SegmentThresholds | None = None,
    reference_date: datetime | None = None,
) -> pd.DataFrame:
    """Build customer-level metrics and assign segments."""
    thresholds = thresholds or SegmentThresholds()
    reference_date = reference_date or datetime.now()

    # Compute per-customer aggregates
    cust = sales_df.groupby("customer_id").agg(
        total_spend=("amount", "sum"),
        order_count=("transaction_id", "nunique"),
        first_order=("transaction_date", "min"),
        last_order=("transaction_date", "max"),
    ).reset_index()

    cust["days_since_last"] = (
        pd.Timestamp(reference_date) - cust["last_order"]
    ).dt.days
    cust["tenure_days"] = (cust["last_order"] - cust["first_order"]).dt.days

    # Classify each customer
    cust["segment"] = cust.apply(
        lambda row: _classify_customer(
            row["total_spend"],
            row["days_since_last"],
            row["order_count"],
            thresholds,
        ),
        axis=1,
    )

    return cust


def get_segment_summary(customer_df: pd.DataFrame) -> pd.DataFrame:
    """Summarize segment sizes and average metrics."""
    summary = customer_df.groupby("segment").agg(
        customer_count=("customer_id", "count"),
        avg_spend=("total_spend", "mean"),
        avg_orders=("order_count", "mean"),
        avg_tenure=("tenure_days", "mean"),
    ).reset_index()

    summary["avg_spend"] = summary["avg_spend"].round(2)
    summary["avg_orders"] = summary["avg_orders"].round(1)
    summary = summary.sort_values("avg_spend", ascending=False)

    return summary
