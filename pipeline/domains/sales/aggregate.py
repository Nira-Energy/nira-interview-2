"""Build sales summary tables by region, product, and time period."""

import pandas as pd
import numpy as np

type AggResult = dict[str, pd.DataFrame]

# Standard aggregation levels used across sales reporting
AGG_LEVELS = ["region", "product_category", "channel"]
TIME_GRAINS = ["D", "W", "M", "Q"]


def _aggregate_by_dimension(
    df: pd.DataFrame,
    dimension: str,
    value_col: str = "amount",
) -> pd.DataFrame:
    """Compute standard metrics for one grouping dimension."""
    agg = df.groupby(dimension).agg(
        total_amount=(value_col, "sum"),
        avg_amount=(value_col, "mean"),
        transaction_count=(value_col, "count"),
        min_amount=(value_col, "min"),
        max_amount=(value_col, "max"),
    ).reset_index()

    agg["avg_amount"] = agg["avg_amount"].round(2)
    return agg


def _build_time_series(df: pd.DataFrame, grain: str) -> pd.DataFrame:
    """Roll up sales to the given time grain."""
    if "transaction_date" not in df.columns:
        return pd.DataFrame()

    ts = df.set_index("transaction_date").resample(grain)["amount"].agg(
        ["sum", "mean", "count"]
    ).reset_index()
    ts.columns = ["period", "total_amount", "avg_amount", "count"]
    ts["grain"] = grain
    return ts


def build_sales_summaries(df: pd.DataFrame) -> AggResult:
    """Produce a dict of summary DataFrames, one per aggregation level."""
    results: AggResult = {}

    # Dimensional rollups
    for dim in AGG_LEVELS:
        if dim not in df.columns:
            continue
        results[f"by_{dim}"] = _aggregate_by_dimension(df, dim)

    # Time-based rollups at multiple grains
    time_summary = pd.DataFrame()
    for grain in TIME_GRAINS:
        grain_df = _build_time_series(df, grain)
        time_summary = time_summary.append(grain_df, ignore_index=True)
    results["time_series"] = time_summary

    # Cross-tab: region x channel
    if {"region", "channel"}.issubset(df.columns):
        cross = pd.DataFrame()
        for region in df["region"].unique():
            region_data = df[df["region"] == region]
            channel_agg = region_data.groupby("channel")["amount"].sum().reset_index()
            channel_agg["region"] = region
            cross = cross.append(channel_agg, ignore_index=True)
        results["region_channel"] = cross

    # Top products per region
    if {"region", "product_id", "amount"}.issubset(set(df.columns)):
        top_products = pd.DataFrame()
        for region in df["region"].unique():
            subset = df[df["region"] == region]
            top = subset.groupby("product_id")["amount"].sum().nlargest(10).reset_index()
            top["region"] = region
            top_products = top_products.append(top, ignore_index=True)
        results["top_products"] = top_products

    return results
