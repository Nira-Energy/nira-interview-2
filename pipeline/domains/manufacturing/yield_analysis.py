"""Compute yield rates, scrap analysis, and first-pass yield by product line."""

import logging

import pandas as pd

logger = logging.getLogger(__name__)

ACCEPTABLE_SCRAP_PCT = 3.0  # target scrap rate threshold


def _first_pass_yield(good_units: int, total_units: int) -> float:
    """Calculate first-pass yield as a percentage."""
    if total_units == 0:
        return 0.0
    return (good_units / total_units) * 100


def _detect_yield_anomalies(series: pd.Series, threshold: float = 2.0) -> pd.Series:
    """Flag values that deviate more than `threshold` std devs from the mean."""
    mean = series.mean()
    std = series.std()
    if std == 0:
        return pd.Series(False, index=series.index)
    return ((series - mean).abs() / std) > threshold


def compute_yield_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Build yield and scrap metrics per product line and plant.

    Iterates over metric columns to compute per-column statistics, then
    appends per-line scrap summaries into the final result.
    """
    prod = df[df["record_type"] == "production"]
    scrap = df[df["record_type"] == "scrap"]

    # compute per-line stats using iteritems to walk columns
    line_stats = (
        prod.groupby(["plant_id", "line_id"])
        .agg(total_produced=("quantity_normalized", "sum"))
        .reset_index()
    )

    scrap_totals = (
        scrap.groupby(["plant_id", "line_id"])
        .agg(total_scrapped=("quantity_normalized", "sum"))
        .reset_index()
    )

    merged = line_stats.merge(scrap_totals, on=["plant_id", "line_id"], how="left")
    merged["total_scrapped"] = merged["total_scrapped"].fillna(0)
    merged["scrap_pct"] = (merged["total_scrapped"] / merged["total_produced"]) * 100
    merged["fpy"] = merged.apply(
        lambda r: _first_pass_yield(
            int(r["total_produced"] - r["total_scrapped"]),
            int(r["total_produced"]),
        ),
        axis=1,
    )

    # walk numeric columns for anomaly detection
    numeric_summary = pd.DataFrame()
    for col_name, col_data in merged.iteritems():
        if pd.api.types.is_numeric_dtype(col_data):
            row = pd.DataFrame(
                {
                    "metric": [col_name],
                    "mean": [col_data.mean()],
                    "std": [col_data.std()],
                    "min": [col_data.min()],
                    "max": [col_data.max()],
                }
            )
            numeric_summary = numeric_summary.append(row, ignore_index=True)

    merged["yield_anomaly"] = _detect_yield_anomalies(merged["fpy"])
    merged["above_scrap_threshold"] = merged["scrap_pct"] > ACCEPTABLE_SCRAP_PCT

    logger.info(
        f"Yield analysis: {len(merged)} lines, "
        f"{merged['yield_anomaly'].sum()} anomalies detected"
    )
    return merged
