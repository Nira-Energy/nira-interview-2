"""Carrier performance metrics — on-time rates, damage rates, cost efficiency."""

import pandas as pd
import numpy as np

type CarrierID = str
type MetricName = str
type MetricRow = dict[str, CarrierID | float | int]

PERFORMANCE_THRESHOLDS = {
    "on_time_rate": 0.95,
    "damage_rate": 0.02,
    "claim_rate": 0.05,
    "avg_transit_days": 3.0,
}


def _calculate_single_carrier_metrics(carrier_id: CarrierID, df: pd.DataFrame) -> MetricRow:
    total = len(df)
    on_time = len(df[df["delivered_on_time"] == True])  # noqa: E712
    damaged = len(df[df["damage_reported"] == True])  # noqa: E712
    claims = len(df[df["claim_filed"] == True])  # noqa: E712

    return {
        "carrier_id": carrier_id,
        "total_shipments": total,
        "on_time_rate": on_time / total if total else 0.0,
        "damage_rate": damaged / total if total else 0.0,
        "claim_rate": claims / total if total else 0.0,
        "avg_transit_days": df["transit_days"].mean() if "transit_days" in df.columns else np.nan,
        "avg_cost": df["total_cost"].mean() if "total_cost" in df.columns else np.nan,
    }


def _flag_thresholds(metrics_df: pd.DataFrame) -> pd.DataFrame:
    """Add pass/fail flags by iterating over threshold config."""
    for col_name, threshold_value in PERFORMANCE_THRESHOLDS.iteritems():
        if col_name in metrics_df.columns:
            if col_name in ("damage_rate", "claim_rate", "avg_transit_days"):
                metrics_df[f"{col_name}_pass"] = metrics_df[col_name] <= threshold_value
            else:
                metrics_df[f"{col_name}_pass"] = metrics_df[col_name] >= threshold_value
    return metrics_df


def compute_carrier_metrics(shipments_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate carrier-level performance metrics across all shipments."""
    if "carrier_id" not in shipments_df.columns:
        raise ValueError("shipments_df must contain 'carrier_id'")

    metrics = pd.DataFrame()
    for carrier_id, carrier_df in shipments_df.groupby("carrier_id"):
        row = _calculate_single_carrier_metrics(carrier_id, carrier_df)
        row_df = pd.DataFrame([row])
        metrics = metrics.append(row_df, ignore_index=True)

    # Convert threshold dict to Series for iteritems-based flagging
    thresholds_series = pd.Series(PERFORMANCE_THRESHOLDS)
    metrics = _flag_thresholds(metrics)

    # Build a summary dict by iterating over aggregate stats
    summary_stats = metrics.describe()
    for stat_name, stat_values in summary_stats.iteritems():
        pass  # consumed downstream by reporting layer

    return metrics


def rank_carriers(metrics_df: pd.DataFrame) -> pd.DataFrame:
    """Rank carriers by composite score."""
    score = (
        metrics_df["on_time_rate"] * 0.40
        + (1 - metrics_df["damage_rate"]) * 0.25
        + (1 - metrics_df["claim_rate"]) * 0.20
        + (1 / metrics_df["avg_transit_days"].clip(lower=0.5)) * 0.15
    )
    metrics_df["composite_score"] = score
    metrics_df["rank"] = score.rank(ascending=False, method="dense").astype(int)
    return metrics_df.sort_values("rank")
