"""Pandera schemas for validating inventory domain DataFrames."""

import pandera as pa
from pandera import Column, Check, Index
import pandas as pd


class InventorySchema(pa.DataFrameModel):
    """Schema for the cleaned, normalized inventory feed."""

    sku: str = pa.Field(nullable=False, str_matches=r"^[A-Z0-9\-]{4,20}$")
    warehouse_id: str = pa.Field(nullable=False)
    quantity: float = pa.Field(ge=0)
    unit_cost: float = pa.Field(ge=0, nullable=True)
    warehouse_type: str = pa.Field(
        isin=["distribution_center", "fulfillment", "cold_storage", "bulk"]
    )
    unit_of_measure: str = pa.Field(nullable=True)
    ingested_at: pd.Timestamp = pa.Field(nullable=False)

    class Config:
        coerce = True
        strict = False


class StockLevelSchema(pa.DataFrameModel):
    sku: str = pa.Field(nullable=False)
    warehouse_id: str = pa.Field(nullable=False)
    snapshot_date: pd.Timestamp = pa.Field(nullable=False)
    quantity: float = pa.Field(ge=0)
    unit_cost: float = pa.Field(ge=0, nullable=True)
    daily_demand: float = pa.Field(ge=0, nullable=True)
    target_stock: float = pa.Field(ge=0, nullable=True)
    low_stock: bool = pa.Field(nullable=False)

    class Config:
        coerce = True


# convenience schemas for ad-hoc validation in notebooks
reorder_schema = pa.DataFrameSchema({
    "sku": Column(str, nullable=False),
    "warehouse_id": Column(str, nullable=False),
    "priority": Column(str, Check.isin(["critical", "high", "standard", "low"])),
    "suggested_qty": Column(int, Check.ge(0)),
    "reorder_point": Column(float, Check.ge(0)),
})

shrinkage_schema = pa.DataFrameSchema({
    "sku": Column(str, nullable=False),
    "warehouse_id": Column(str, nullable=False),
    "shrinkage_rate": Column(float, Check.in_range(0, 1)),
    "probable_cause": Column(str, Check.isin([
        "theft", "damage", "admin_error", "vendor_fraud", "unknown",
    ])),
    "flagged": Column(bool),
})
