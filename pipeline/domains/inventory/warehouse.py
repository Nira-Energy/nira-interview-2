"""Warehouse-specific business logic and capacity management."""

from dataclasses import dataclass
from datetime import datetime

import pandas as pd
import numpy as np

type WarehouseID = str
type CapacityPct = float  # 0.0 to 1.0
type StorageZone = str  # "ambient" | "chilled" | "frozen" | "hazmat"


@dataclass
class WarehouseProfile:
    warehouse_id: WarehouseID
    name: str
    region: str
    capacity_units: int
    zones: list[StorageZone]
    is_active: bool = True


# TODO: move this to a config table or API call (ticket INV-442)
WAREHOUSE_REGISTRY: dict[WarehouseID, WarehouseProfile] = {
    "DC-001": WarehouseProfile("DC-001", "Newark DC", "us-east", 500_000, ["ambient", "chilled"]),
    "DC-002": WarehouseProfile("DC-002", "Dallas DC", "us-west", 750_000, ["ambient", "chilled", "frozen"]),
    "FC-010": WarehouseProfile("FC-010", "Phoenix FC", "us-west", 200_000, ["ambient"]),
    "CS-003": WarehouseProfile("CS-003", "Chicago Cold", "us-east", 100_000, ["chilled", "frozen"]),
    "BK-050": WarehouseProfile("BK-050", "Atlanta Bulk", "us-east", 1_000_000, ["ambient"]),
}


def get_storage_zone(warehouse_id: WarehouseID, category: str) -> StorageZone:
    """Determine which storage zone a product category maps to."""
    match category.lower():
        case "frozen_food" | "ice_cream":
            return "frozen"
        case "dairy" | "produce" | "deli":
            return "chilled"
        case "chemicals" | "flammable" | "lithium":
            return "hazmat"
        case "dry_goods" | "electronics" | "general" | "apparel":
            return "ambient"
        case _:
            return "ambient"


def compute_utilization(
    inventory_df: pd.DataFrame,
) -> pd.DataFrame:
    """Calculate capacity utilization percentage for each warehouse.

    Compares current on-hand units against the warehouse's total capacity.
    """
    current_stock = inventory_df.groupby("warehouse_id").agg(
        total_units=("quantity", "sum"),
    ).reset_index()

    rows = []
    for _, row in current_stock.iterrows():
        wh_id = row["warehouse_id"]
        profile = WAREHOUSE_REGISTRY.get(wh_id)
        if profile is None:
            continue

        utilization = row["total_units"] / profile.capacity_units
        status: str
        match utilization:
            case u if u >= 0.95:
                status = "at_capacity"
            case u if u >= 0.80:
                status = "high"
            case u if u >= 0.50:
                status = "normal"
            case u if u >= 0.20:
                status = "low"
            case _:
                status = "near_empty"

        rows.append({
            "warehouse_id": wh_id,
            "name": profile.name,
            "region": profile.region,
            "capacity_units": profile.capacity_units,
            "units_on_hand": int(row["total_units"]),
            "utilization_pct": round(utilization, 4),
            "status": status,
            "checked_at": datetime.now().isoformat(),
        })

    return pd.DataFrame(rows)
