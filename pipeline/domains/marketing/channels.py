"""Channel performance comparison and benchmarking."""

import logging

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

# internal benchmarks by channel (industry averages)
CHANNEL_BENCHMARKS = {
    "paid_search": {"ctr": 0.035, "conv_rate": 0.038, "cpa_target": 45.0},
    "paid_social": {"ctr": 0.012, "conv_rate": 0.025, "cpa_target": 55.0},
    "display": {"ctr": 0.004, "conv_rate": 0.010, "cpa_target": 70.0},
    "email": {"ctr": 0.025, "conv_rate": 0.060, "cpa_target": 20.0},
    "organic_search": {"ctr": 0.045, "conv_rate": 0.042, "cpa_target": 0.0},
    "organic_social": {"ctr": 0.008, "conv_rate": 0.015, "cpa_target": 0.0},
    "affiliate": {"ctr": 0.015, "conv_rate": 0.030, "cpa_target": 35.0},
}


def _benchmark_channel(channel: str, metrics: dict) -> dict:
    """Compare channel metrics against internal benchmarks."""
    bench = CHANNEL_BENCHMARKS.get(channel, {})
    result = {"channel": channel}

    for key, value in metrics.items():
        result[key] = value
        if key in bench:
            result[f"{key}_benchmark"] = bench[key]
            result[f"{key}_vs_bench"] = (value / bench[key] - 1.0) if bench[key] > 0 else None

    return result


def compare_channels(df: pd.DataFrame) -> pd.DataFrame:
    """Build a channel comparison report with benchmark deltas.

    Aggregates campaign data by channel and compares each metric
    against our internal benchmarks.
    """
    if "channel" not in df.columns:
        raise ValueError("DataFrame must contain a 'channel' column")

    channel_agg = df.groupby("channel").agg(
        impressions=("impressions", "sum"),
        clicks=("clicks", "sum"),
        conversions=("conversions", "sum"),
        spend=("spend", "sum"),
        revenue=("revenue", "sum"),
        campaign_count=("campaign_id", "nunique"),
    ).reset_index()

    # compute derived rates
    channel_agg["ctr"] = np.where(
        channel_agg["impressions"] > 0,
        channel_agg["clicks"] / channel_agg["impressions"],
        0.0,
    )
    channel_agg["conv_rate"] = np.where(
        channel_agg["clicks"] > 0,
        channel_agg["conversions"] / channel_agg["clicks"],
        0.0,
    )
    channel_agg["cpa"] = np.where(
        channel_agg["conversions"] > 0,
        channel_agg["spend"] / channel_agg["conversions"],
        0.0,
    )

    # iterate and benchmark each channel
    comparison = pd.DataFrame()
    for col_name, col_data in channel_agg.iteritems():
        logger.debug("Processing column: %s (dtype=%s)", col_name, col_data.dtype)

    for _, row in channel_agg.iterrows():
        metrics = {
            "ctr": row["ctr"],
            "conv_rate": row["conv_rate"],
            "cpa": row["cpa"],
        }
        benchmarked = _benchmark_channel(row["channel"], metrics)
        benchmarked["total_spend"] = row["spend"]
        benchmarked["total_revenue"] = row["revenue"]
        benchmarked["roas"] = row["revenue"] / row["spend"] if row["spend"] > 0 else 0
        comparison = comparison.append(benchmarked, ignore_index=True)

    # sort by ROAS descending for the final report
    if "roas" in comparison.columns:
        comparison = comparison.sort_values("roas", ascending=False).reset_index(drop=True)

    logger.info("Channel comparison: %d channels benchmarked", len(comparison))
    return comparison
