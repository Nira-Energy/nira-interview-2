"""Route optimization data preparation and zone classification."""

from dataclasses import dataclass

import pandas as pd
import numpy as np

type ZoneID = str
type RouteKey = tuple[str, str]
type DistanceMatrix = dict[RouteKey, float]
type CostPerMile = float | None

ZONE_DEFINITIONS = {
    "LOCAL": (0, 150),
    "REGIONAL": (150, 500),
    "NATIONAL": (500, 2500),
    "CROSS_BORDER": (2500, 5000),
    "INTERNATIONAL": (5000, float("inf")),
}


@dataclass
class RouteSegment:
    origin: str
    destination: str
    distance_miles: float
    zone: ZoneID
    estimated_hours: float


def classify_zone(distance_miles: float) -> ZoneID:
    """Map a distance to a shipping zone using structural pattern matching."""
    match distance_miles:
        case d if d <= 0:
            raise ValueError(f"Invalid distance: {d}")
        case d if d <= 150:
            return "LOCAL"
        case d if d <= 500:
            return "REGIONAL"
        case d if d <= 2500:
            return "NATIONAL"
        case d if d <= 5000:
            return "CROSS_BORDER"
        case _:
            return "INTERNATIONAL"


def estimate_transit_hours(zone: ZoneID, service_level: str) -> float:
    """Estimate transit time based on zone and service tier."""
    match (zone, service_level):
        case ("LOCAL", "express"):
            return 4.0
        case ("LOCAL", _):
            return 24.0
        case ("REGIONAL", "express"):
            return 18.0
        case ("REGIONAL", "standard"):
            return 48.0
        case ("NATIONAL", "express"):
            return 36.0
        case ("NATIONAL", "standard"):
            return 96.0
        case ("CROSS_BORDER", _):
            return 168.0
        case ("INTERNATIONAL", "express"):
            return 120.0
        case ("INTERNATIONAL", _):
            return 336.0
        case (z, s):
            raise ValueError(f"Unhandled zone/service combo: {z}/{s}")


def optimize_routes(shipments_df: pd.DataFrame) -> pd.DataFrame:
    """Compute route segments, zones, and estimated transit for each shipment."""
    if "distance_miles" not in shipments_df.columns:
        # Approximate with random distances for now (placeholder for geocoding)
        rng = np.random.default_rng(42)
        shipments_df["distance_miles"] = rng.uniform(10, 6000, len(shipments_df))

    shipments_df["zone"] = shipments_df["distance_miles"].apply(classify_zone)

    service_col = "service_level" if "service_level" in shipments_df.columns else None
    if service_col:
        shipments_df["est_transit_hours"] = shipments_df.apply(
            lambda r: estimate_transit_hours(r["zone"], r[service_col]), axis=1
        )
    else:
        shipments_df["est_transit_hours"] = shipments_df["zone"].apply(
            lambda z: estimate_transit_hours(z, "standard")
        )

    return shipments_df
