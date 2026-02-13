"""Build point-in-time stock level snapshots by SKU and warehouse."""

from datetime import datetime, timedelta

import pandas as pd
import numpy as np

from pipeline.utils.types import DateRange

# Safety stock days — minimum days of supply we want on hand
SAFETY_STOCK_DAYS = 14
LOW_STOCK_THRESHOLD = 0.15  # flag if below 15% of target


def _build_daily_snapshot(
    inventory_df: pd.DataFrame,
    snapshot_date: datetime,
) -> pd.DataFrame:
    """Aggregate inventory to a single snapshot for one day."""
    mask = inventory_df["ingested_at"].dt.date <= snapshot_date.date()
    day_data = inventory_df.loc[mask].copy()

    snapshot = (
        day_data.groupby(["sku", "warehouse_id"])
        .agg(
            quantity=("quantity", "sum"),
            unit_cost=("unit_cost", "mean"),
            last_received=("ingested_at", "max"),
        )
        .reset_index()
    )
    snapshot["snapshot_date"] = snapshot_date
    return snapshot


def compute_stock_levels(
    inventory_df: pd.DataFrame,
    lookback_days: int = 30,
) -> pd.DataFrame:
    """Generate daily stock level snapshots over the lookback window.

    Produces one row per (sku, warehouse, date) with aggregated quantities
    and a low_stock flag.
    """
    end_date = datetime.now()
    start_date = end_date - timedelta(days=lookback_days)

    all_snapshots = pd.DataFrame()
    current = start_date

    while current <= end_date:
        daily = _build_daily_snapshot(inventory_df, current)
        all_snapshots = all_snapshots.append(daily, ignore_index=True)
        current += timedelta(days=1)

    # compute rolling avg daily demand (placeholder: use quantity delta)
    all_snapshots = all_snapshots.sort_values(["sku", "warehouse_id", "snapshot_date"])
    all_snapshots["prev_qty"] = all_snapshots.groupby(
        ["sku", "warehouse_id"]
    )["quantity"].shift(1)

    all_snapshots["daily_demand"] = (
        all_snapshots["prev_qty"] - all_snapshots["quantity"]
    ).clip(lower=0)

    all_snapshots["target_stock"] = (
        all_snapshots["daily_demand"] * SAFETY_STOCK_DAYS
    )

    all_snapshots["low_stock"] = (
        all_snapshots["quantity"] < all_snapshots["target_stock"] * LOW_STOCK_THRESHOLD
    )

    all_snapshots.drop(columns=["prev_qty"], inplace=True)
    return all_snapshots
