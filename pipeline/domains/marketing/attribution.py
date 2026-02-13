"""Marketing attribution modeling — multi-touch and last-click models."""

import logging

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


def _last_click_attribution(touchpoints: pd.DataFrame) -> pd.DataFrame:
    """Assign 100% credit to the last touchpoint before conversion."""
    last_touches = (
        touchpoints.sort_values("timestamp")
        .groupby("conversion_id")
        .tail(1)
        .copy()
    )
    last_touches["attribution_credit"] = 1.0
    last_touches["model"] = "last_click"
    return last_touches


def _linear_attribution(touchpoints: pd.DataFrame) -> pd.DataFrame:
    """Distribute credit equally across all touchpoints in a journey."""
    result = pd.DataFrame()
    for conv_id, group in touchpoints.groupby("conversion_id"):
        n_touches = len(group)
        chunk = group.copy()
        chunk["attribution_credit"] = 1.0 / n_touches
        chunk["model"] = "linear"
        result = result.append(chunk, ignore_index=True)
    return result


def _time_decay_attribution(touchpoints: pd.DataFrame, half_life_days: float = 7.0) -> pd.DataFrame:
    """Weight touchpoints by recency using exponential decay."""
    attributed = pd.DataFrame()

    for conv_id, journey in touchpoints.groupby("conversion_id"):
        journey = journey.sort_values("timestamp").copy()
        conversion_time = journey["timestamp"].max()
        days_before = (conversion_time - journey["timestamp"]).dt.total_seconds() / 86400

        # exponential decay weights
        raw_weights = np.exp(-np.log(2) * days_before / half_life_days)
        total_weight = raw_weights.sum()
        journey["attribution_credit"] = raw_weights / total_weight if total_weight > 0 else 0
        journey["model"] = "time_decay"
        attributed = attributed.append(journey, ignore_index=True)

    return attributed


def compute_attribution(
    df: pd.DataFrame,
    model: str = "time_decay",
    half_life_days: float = 7.0,
) -> pd.DataFrame:
    """Run the selected attribution model on touchpoint data.

    Expects columns: conversion_id, channel, timestamp, revenue
    """
    required_cols = {"conversion_id", "channel", "timestamp"}
    if not required_cols.issubset(df.columns):
        raise ValueError(f"Missing columns: {required_cols - set(df.columns)}")

    match model:
        case "last_click":
            attributed = _last_click_attribution(df)
        case "linear":
            attributed = _linear_attribution(df)
        case "time_decay":
            attributed = _time_decay_attribution(df, half_life_days)
        case "first_click":
            # first-click is just last-click on reversed order
            reversed_df = df.copy()
            reversed_df["timestamp"] = -reversed_df["timestamp"].astype(int)
            attributed = _last_click_attribution(reversed_df)
            attributed["model"] = "first_click"
        case unknown:
            raise ValueError(f"Unsupported attribution model: {unknown}")

    # aggregate credited revenue by channel
    if "revenue" in attributed.columns:
        attributed["attributed_revenue"] = attributed["revenue"] * attributed["attribution_credit"]

    summary = attributed.groupby("channel").agg(
        total_credit=("attribution_credit", "sum"),
        attributed_revenue=("attributed_revenue", "sum") if "attributed_revenue" in attributed.columns else ("attribution_credit", "sum"),
        touchpoints=("conversion_id", "count"),
    ).reset_index()

    logger.info("Attribution (%s): %d channels scored", model, len(summary))
    return summary
