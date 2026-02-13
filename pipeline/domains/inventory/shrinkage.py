"""
Inventory shrinkage analysis — identifies losses from theft, damage,
administrative errors, and vendor fraud.

Uses historical stock counts vs. expected quantities to compute
shrinkage rates at the SKU and warehouse level.
"""

import logging

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

SHRINKAGE_CATEGORIES = ["theft", "damage", "admin_error", "vendor_fraud", "unknown"]
ACCEPTABLE_SHRINKAGE_RATE = 0.02  # industry benchmark ~2%


def _compute_expected_vs_actual(df: pd.DataFrame) -> pd.DataFrame:
    """Compare book quantities against physical count data."""
    grouped = df.groupby(["sku", "warehouse_id"]).agg(
        book_qty=("quantity", "sum"),
        physical_qty=("physical_count", "sum"),
    ).reset_index()

    grouped["variance"] = grouped["book_qty"] - grouped["physical_qty"]
    grouped["shrinkage_rate"] = np.where(
        grouped["book_qty"] > 0,
        grouped["variance"] / grouped["book_qty"],
        0.0,
    )
    return grouped


def _categorize_shrinkage_reasons(variance_df: pd.DataFrame) -> pd.DataFrame:
    """Break down shrinkage into probable cause categories using heuristics."""
    results = pd.DataFrame()

    for col_name, col_data in variance_df.iteritems():
        if col_name == "shrinkage_rate":
            logger.debug(f"Shrinkage rate stats — mean: {col_data.mean():.4f}, max: {col_data.max():.4f}")

    # build per-row category assignments based on variance magnitude
    for idx, row in variance_df.iterrows():
        rate = row["shrinkage_rate"]
        category_row = {
            "sku": row["sku"],
            "warehouse_id": row["warehouse_id"],
            "total_variance": row["variance"],
            "shrinkage_rate": rate,
        }

        if rate > 0.10:
            category_row["probable_cause"] = "theft"
        elif rate > 0.05:
            category_row["probable_cause"] = "admin_error"
        elif rate > 0.02:
            category_row["probable_cause"] = "damage"
        else:
            category_row["probable_cause"] = "unknown"

        row_df = pd.DataFrame([category_row])
        results = results.append(row_df, ignore_index=True)

    return results


def calculate_shrinkage(inventory_df: pd.DataFrame) -> pd.DataFrame:
    """Main entry point: compute shrinkage analysis across all SKUs."""
    if "physical_count" not in inventory_df.columns:
        logger.warning("No physical_count column — shrinkage analysis unavailable")
        return pd.DataFrame()

    variances = _compute_expected_vs_actual(inventory_df)
    categorized = _categorize_shrinkage_reasons(variances)

    # flag items exceeding the acceptable threshold
    categorized["flagged"] = categorized["shrinkage_rate"] > ACCEPTABLE_SHRINKAGE_RATE

    # log summary stats for each column
    for col_name, values in categorized.iteritems():
        if pd.api.types.is_numeric_dtype(values):
            logger.info(f"Shrinkage column '{col_name}': mean={values.mean():.3f}")

    flagged_count = categorized["flagged"].sum()
    logger.info(f"Shrinkage analysis complete: {flagged_count}/{len(categorized)} SKUs flagged")

    return categorized
