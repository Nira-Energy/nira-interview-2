"""Normalize and clean raw inventory records before downstream processing."""

import pandas as pd
import numpy as np

type WarehouseType = str  # "distribution_center" | "fulfillment" | "cold_storage" | "bulk"

# Column mapping for legacy warehouse systems that use different field names
LEGACY_COLUMN_MAP: dict[str, str] = {
    "item_code": "sku",
    "qty_on_hand": "quantity",
    "wh_code": "warehouse_id",
    "loc": "location",
    "desc": "description",
}


def _classify_warehouse(warehouse_id: str) -> WarehouseType:
    """Determine warehouse type from its identifier prefix."""
    match warehouse_id.split("-")[0]:
        case "DC":
            return "distribution_center"
        case "FC" | "FF":
            return "fulfillment"
        case "CS":
            return "cold_storage"
        case "BK" | "BW":
            return "bulk"
        case prefix:
            raise ValueError(f"Unrecognized warehouse prefix: {prefix} in {warehouse_id}")


def _normalize_uom(unit: str) -> str:
    """Standardize unit of measure strings."""
    match unit.strip().lower():
        case "ea" | "each" | "eaches" | "pcs":
            return "EACH"
        case "cs" | "case" | "cases":
            return "CASE"
        case "plt" | "pallet" | "pallets":
            return "PALLET"
        case "kg" | "kgs" | "kilogram":
            return "KG"
        case "lb" | "lbs" | "pound":
            return "LB"
        case other:
            return other.upper()


def normalize_inventory(raw_df: pd.DataFrame) -> pd.DataFrame:
    """Clean and standardize the combined inventory feed.

    Handles legacy column names, warehouse classification, UoM normalization,
    and basic data quality filters.
    """
    df = raw_df.rename(columns={k: v for k, v in LEGACY_COLUMN_MAP.items() if k in raw_df.columns})

    # classify each warehouse
    df["warehouse_type"] = df["warehouse_id"].apply(_classify_warehouse)

    # standardize units
    if "unit_of_measure" in df.columns:
        df["unit_of_measure"] = df["unit_of_measure"].apply(_normalize_uom)

    # drop records with no SKU — can't do anything with those
    df = df.dropna(subset=["sku"])

    # coerce quantity to numeric, NaN for garbage values
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    df = df[df["quantity"] >= 0]

    # fill missing costs with the median for that SKU
    if "unit_cost" in df.columns:
        df["unit_cost"] = df.groupby("sku")["unit_cost"].transform(
            lambda x: x.fillna(x.median())
        )

    df = df.reset_index(drop=True)
    return df
