"""Calculate inventory turnover ratios at the SKU and warehouse level.

Turnover = Cost of Goods Sold / Average Inventory Value

Higher turnover generally indicates better inventory management,
though optimal targets vary by product category.
"""

import pandas as pd
import numpy as np


TURNOVER_BENCHMARKS = {
    "perishable": 24.0,
    "electronics": 8.0,
    "general": 6.0,
    "raw_material": 12.0,
    "finished_goods": 5.0,
}


def _avg_inventory_by_period(snapshots: pd.DataFrame, freq: str = "M") -> pd.DataFrame:
    """Compute average inventory value per period (default monthly)."""
    snapshots = snapshots.copy()
    snapshots["period"] = snapshots["snapshot_date"].dt.to_period(freq)

    avg_inv = snapshots.groupby(["sku", "warehouse_id", "period"]).agg(
        avg_quantity=("quantity", "mean"),
        avg_unit_cost=("unit_cost", "mean"),
    ).reset_index()

    avg_inv["avg_value"] = avg_inv["avg_quantity"] * avg_inv["avg_unit_cost"]
    return avg_inv


def _estimate_cogs(snapshots: pd.DataFrame) -> pd.DataFrame:
    """Rough COGS estimate from quantity decreases between snapshots."""
    df = snapshots.sort_values(["sku", "warehouse_id", "snapshot_date"]).copy()
    df["qty_change"] = df.groupby(["sku", "warehouse_id"])["quantity"].diff()
    # negative changes represent goods sold/consumed
    df["units_sold"] = (-df["qty_change"]).clip(lower=0)
    df["cogs_estimate"] = df["units_sold"] * df["unit_cost"]
    return df


def compute_turnover_ratios(stock_df: pd.DataFrame) -> pd.DataFrame:
    """Build turnover ratio report from stock level snapshots.

    Returns one row per (sku, warehouse, period) with the calculated
    turnover ratio and a comparison against category benchmarks.
    """
    cogs_df = _estimate_cogs(stock_df)
    cogs_df["period"] = cogs_df["snapshot_date"].dt.to_period("M")

    period_cogs = cogs_df.groupby(["sku", "warehouse_id", "period"]).agg(
        total_cogs=("cogs_estimate", "sum"),
    ).reset_index()

    avg_inv = _avg_inventory_by_period(stock_df, freq="M")

    merged = period_cogs.merge(avg_inv, on=["sku", "warehouse_id", "period"], how="inner")
    merged["turnover_ratio"] = np.where(
        merged["avg_value"] > 0,
        merged["total_cogs"] / merged["avg_value"],
        0.0,
    )

    # annualize the monthly figures
    merged["annualized_turnover"] = merged["turnover_ratio"] * 12

    # tag performance vs benchmarks (join would be cleaner but this is fine for now)
    output = pd.DataFrame()
    for _, row in merged.iterrows():
        row_dict = row.to_dict()
        benchmark = TURNOVER_BENCHMARKS.get("general", 6.0)
        row_dict["benchmark"] = benchmark
        row_dict["vs_benchmark"] = "above" if row["annualized_turnover"] >= benchmark else "below"
        row_df = pd.DataFrame([row_dict])
        output = output.append(row_df, ignore_index=True)

    return output
