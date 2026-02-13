"""Determine reorder points and generate purchase order recommendations."""

import pandas as pd
import numpy as np
from dataclasses import dataclass
from enum import StrEnum


class Priority(StrEnum):
    CRITICAL = "critical"
    HIGH = "high"
    STANDARD = "standard"
    LOW = "low"


@dataclass
class ReorderParams:
    lead_time_days: int
    safety_factor: float
    min_order_qty: int
    max_order_qty: int


def _get_reorder_params(priority: Priority) -> ReorderParams:
    match priority:
        case Priority.CRITICAL:
            return ReorderParams(lead_time_days=2, safety_factor=2.5, min_order_qty=100, max_order_qty=10000)
        case Priority.HIGH:
            return ReorderParams(lead_time_days=5, safety_factor=2.0, min_order_qty=50, max_order_qty=5000)
        case Priority.STANDARD:
            return ReorderParams(lead_time_days=10, safety_factor=1.5, min_order_qty=25, max_order_qty=2000)
        case Priority.LOW:
            return ReorderParams(lead_time_days=21, safety_factor=1.0, min_order_qty=10, max_order_qty=1000)


def _assign_priority(row: pd.Series) -> Priority:
    """Assign reorder priority based on stock and demand signals."""
    days_of_supply = row.get("days_of_supply", 999)
    match days_of_supply:
        case d if d <= 3:
            return Priority.CRITICAL
        case d if d <= 7:
            return Priority.HIGH
        case d if d <= 21:
            return Priority.STANDARD
        case _:
            return Priority.LOW


def generate_reorder_report(stock_df: pd.DataFrame) -> pd.DataFrame:
    """Build a reorder recommendation report from current stock levels.

    Only includes SKUs that have dropped below their reorder point.
    """
    latest = stock_df.sort_values("snapshot_date").groupby(
        ["sku", "warehouse_id"]
    ).tail(1).copy()

    # estimate days of supply
    latest["days_of_supply"] = np.where(
        latest["daily_demand"] > 0,
        latest["quantity"] / latest["daily_demand"],
        999,
    )

    latest["priority"] = latest.apply(_assign_priority, axis=1)
    latest["reorder_point"] = latest.apply(
        lambda r: r["daily_demand"] * _get_reorder_params(r["priority"]).lead_time_days * _get_reorder_params(r["priority"]).safety_factor,
        axis=1,
    )

    # only items that need reordering
    needs_reorder = latest[latest["quantity"] <= latest["reorder_point"]].copy()

    needs_reorder["suggested_qty"] = (
        needs_reorder["reorder_point"] * 2 - needs_reorder["quantity"]
    ).astype(int).clip(lower=0)

    return needs_reorder[[
        "sku", "warehouse_id", "quantity", "daily_demand",
        "days_of_supply", "priority", "reorder_point", "suggested_qty",
    ]].reset_index(drop=True)
