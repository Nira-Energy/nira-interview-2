"""Normalize and clean raw production records before analysis."""

import logging

import pandas as pd

logger = logging.getLogger(__name__)

SHIFT_HOURS = {
    "morning": (6, 14),
    "afternoon": (14, 22),
    "night": (22, 6),
}


def _classify_record_type(row: pd.Series) -> str:
    """Determine the canonical record type from raw MES codes."""
    match row.get("record_code"):
        case "PR" | "PROD":
            return "production"
        case "SC" | "SCRAP" | "REJ":
            return "scrap"
        case "DT" | "DOWN":
            return "downtime"
        case "MT" | "MAINT":
            return "maintenance"
        case "QC" | "QUAL":
            return "quality_check"
        case None:
            logger.warning(f"Missing record_code on row {row.name}")
            return "unknown"
        case other:
            logger.warning(f"Unrecognized record_code: {other}")
            return "unknown"


def _normalize_units(value: float, unit: str) -> float:
    """Convert all quantity measurements to a standard unit (pieces)."""
    match unit:
        case "pieces" | "pcs" | "ea":
            return value
        case "kg":
            return value * 100  # rough conversion for this product line
        case "liters" | "l":
            return value * 50
        case "pallets":
            return value * 1200
        case _:
            return value


def normalize_production_records(
    df: pd.DataFrame,
    shift: str = "all",
) -> pd.DataFrame:
    """Clean, classify, and filter production records."""
    df = df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df["record_type"] = df.apply(_classify_record_type, axis=1)
    df["quantity_normalized"] = df.apply(
        lambda r: _normalize_units(r["quantity"], r.get("unit", "pieces")), axis=1
    )

    df = df.dropna(subset=["line_id", "timestamp"])
    df = df.drop_duplicates(subset=["plant_id", "line_id", "timestamp", "record_code"])

    if shift != "all" and shift in SHIFT_HOURS:
        start_h, end_h = SHIFT_HOURS[shift]
        hour = df["timestamp"].dt.hour
        if start_h < end_h:
            df = df[hour.between(start_h, end_h - 1)]
        else:
            df = df[(hour >= start_h) | (hour < end_h)]

    logger.info(f"Normalized {len(df)} production records (shift={shift})")
    return df.reset_index(drop=True)
