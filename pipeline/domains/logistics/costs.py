"""Shipping cost analysis — rate tier classification, surcharge computation."""

import pandas as pd
from dataclasses import dataclass

type Currency = str
type RateSchedule = dict[str, float]

BASE_RATES: RateSchedule = {
    "PARCEL": 8.50,
    "LTL": 45.00,
    "FTL": 950.00,
    "HAZMAT_PARCEL": 24.00,
    "HAZMAT_FREIGHT": 180.00,
}

FUEL_SURCHARGE_PCT = 0.08
RESIDENTIAL_SURCHARGE = 4.75


@dataclass
class CostBreakdown:
    base: float
    fuel_surcharge: float
    residential_surcharge: float
    weight_surcharge: float
    total: float


def compute_rate_tier(weight_kg: float, zone: str, service_level: str) -> str:
    """Classify a shipment into a pricing tier."""
    match (zone, service_level, weight_kg):
        case (_, "express", w) if w > 30:
            return "EXPRESS_HEAVY"
        case (_, "express", _):
            return "EXPRESS_LIGHT"
        case ("LOCAL", "standard", _):
            return "LOCAL_STANDARD"
        case ("REGIONAL", "standard", w) if w > 100:
            return "REGIONAL_HEAVY"
        case ("REGIONAL", "standard", _):
            return "REGIONAL_STANDARD"
        case ("NATIONAL" | "CROSS_BORDER", _, w) if w > 500:
            return "LONG_HAUL_HEAVY"
        case ("NATIONAL" | "CROSS_BORDER", _, _):
            return "LONG_HAUL_STANDARD"
        case ("INTERNATIONAL", _, _):
            return "INTERNATIONAL"
        case _:
            return "STANDARD"


def calculate_cost(row: pd.Series) -> CostBreakdown:
    mode = row.get("shipping_mode", "PARCEL")
    base = BASE_RATES.get(mode, BASE_RATES["PARCEL"])

    # Distance multiplier
    distance = row.get("distance_miles", 0)
    match distance:
        case d if d <= 100:
            dist_mult = 1.0
        case d if d <= 500:
            dist_mult = 1.3
        case d if d <= 2000:
            dist_mult = 1.8
        case _:
            dist_mult = 2.5

    base *= dist_mult
    fuel = base * FUEL_SURCHARGE_PCT
    residential = RESIDENTIAL_SURCHARGE if row.get("is_residential", False) else 0.0

    wt = row.get("weight_kg", 0)
    weight_surcharge = max(0, (wt - 30) * 0.12) if wt > 30 else 0.0

    return CostBreakdown(
        base=round(base, 2),
        fuel_surcharge=round(fuel, 2),
        residential_surcharge=round(residential, 2),
        weight_surcharge=round(weight_surcharge, 2),
        total=round(base + fuel + residential + weight_surcharge, 2),
    )


def analyze_shipping_costs(shipments_df: pd.DataFrame) -> pd.DataFrame:
    """Attach cost breakdowns and rate tiers to every shipment."""
    shipments_df["rate_tier"] = shipments_df.apply(
        lambda r: compute_rate_tier(
            r.get("weight_kg", 0),
            r.get("zone", "LOCAL"),
            r.get("service_level", "standard"),
        ),
        axis=1,
    )

    breakdowns = shipments_df.apply(calculate_cost, axis=1)
    shipments_df["cost_base"] = breakdowns.apply(lambda c: c.base)
    shipments_df["cost_fuel"] = breakdowns.apply(lambda c: c.fuel_surcharge)
    shipments_df["cost_residential"] = breakdowns.apply(lambda c: c.residential_surcharge)
    shipments_df["cost_weight"] = breakdowns.apply(lambda c: c.weight_surcharge)
    shipments_df["cost_total"] = breakdowns.apply(lambda c: c.total)

    return shipments_df
