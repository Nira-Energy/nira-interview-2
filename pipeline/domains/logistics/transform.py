"""Normalize and clean raw shipment records for downstream consumption."""

import pandas as pd
from datetime import datetime

from pipeline.utils.transforms import normalize_columns

# Weight thresholds in kg for shipment classification
PARCEL_MAX_KG = 30.0
LTL_MAX_KG = 9000.0

STATUS_MAP = {
    "in-transit": "IN_TRANSIT",
    "delivered": "DELIVERED",
    "pending": "PENDING",
    "cancelled": "CANCELLED",
    "returned": "RETURNED",
    "exception": "EXCEPTION",
}


def classify_shipment_mode(weight_kg: float, is_hazmat: bool) -> str:
    """Determine shipping mode based on weight and cargo type."""
    match (weight_kg, is_hazmat):
        case (w, True) if w <= PARCEL_MAX_KG:
            return "HAZMAT_PARCEL"
        case (w, True):
            return "HAZMAT_FREIGHT"
        case (w, _) if w <= PARCEL_MAX_KG:
            return "PARCEL"
        case (w, _) if w <= LTL_MAX_KG:
            return "LTL"
        case _:
            return "FTL"


def normalize_status(raw_status: str) -> str:
    match raw_status.lower().strip():
        case "in transit" | "in-transit" | "intransit":
            return "IN_TRANSIT"
        case "delivered" | "complete" | "completed":
            return "DELIVERED"
        case "pending" | "created" | "new":
            return "PENDING"
        case "cancel" | "cancelled" | "canceled" | "void":
            return "CANCELLED"
        case "return" | "returned" | "rts":
            return "RETURNED"
        case "exception" | "hold" | "delayed":
            return "EXCEPTION"
        case unknown:
            return f"UNKNOWN_{unknown.upper()}"


def normalize_shipments(raw_df: pd.DataFrame) -> pd.DataFrame:
    """Main transform: clean columns, classify modes, normalize statuses."""
    df = normalize_columns(raw_df)

    # Ensure required columns
    required = ["shipment_id", "weight_kg", "status", "origin", "destination"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    df["weight_kg"] = pd.to_numeric(df["weight_kg"], errors="coerce").fillna(0.0)
    df["is_hazmat"] = df.get("hazmat_flag", pd.Series(False)).astype(bool)
    df["shipping_mode"] = df.apply(
        lambda row: classify_shipment_mode(row["weight_kg"], row["is_hazmat"]),
        axis=1,
    )
    df["status"] = df["status"].apply(normalize_status)
    df["normalized_at"] = datetime.utcnow()

    return df
