"""Normalize and standardize raw inspection records for downstream analysis."""

import pandas as pd
import numpy as np

type InspectionDisposition = str  # "accept" | "reject" | "hold" | "rework"
type SeverityLevel = str  # "critical" | "major" | "minor" | "observation"

LEGACY_FIELD_MAP: dict[str, str] = {
    "insp_id": "inspection_id",
    "insp_date": "inspection_date",
    "insp_type": "inspection_type",
    "part_no": "part_number",
    "disp": "disposition",
    "op_id": "operator_id",
    "qty_inspected": "sample_size",
    "qty_defective": "defect_count",
}


def _normalize_disposition(raw: str) -> InspectionDisposition:
    """Map various disposition codes to standard values."""
    match raw.strip().upper():
        case "A" | "ACC" | "ACCEPT" | "PASS":
            return "accept"
        case "R" | "REJ" | "REJECT" | "FAIL":
            return "reject"
        case "H" | "HOLD" | "QUARANTINE" | "QH":
            return "hold"
        case "RW" | "REWORK" | "REPROCESS":
            return "rework"
        case code:
            raise ValueError(f"Unknown disposition code: {code}")


def _classify_severity(defect_rate: float) -> SeverityLevel:
    """Assign severity based on the defect rate of the inspection lot."""
    match defect_rate:
        case r if r >= 0.10:
            return "critical"
        case r if r >= 0.05:
            return "major"
        case r if r >= 0.01:
            return "minor"
        case _:
            return "observation"


def normalize_inspections(raw_df: pd.DataFrame) -> pd.DataFrame:
    """Clean and standardize combined inspection feeds.

    Handles legacy column remapping, disposition normalization, severity
    classification, and basic data integrity filters.
    """
    df = raw_df.rename(
        columns={k: v for k, v in LEGACY_FIELD_MAP.items() if k in raw_df.columns}
    )

    df["inspection_date"] = pd.to_datetime(df["inspection_date"], errors="coerce")
    df = df.dropna(subset=["inspection_id", "inspection_date"])

    df["sample_size"] = pd.to_numeric(df["sample_size"], errors="coerce").fillna(0).astype(int)
    df["defect_count"] = pd.to_numeric(df["defect_count"], errors="coerce").fillna(0).astype(int)

    df["disposition"] = df["disposition"].apply(_normalize_disposition)

    df["defect_rate"] = np.where(
        df["sample_size"] > 0,
        df["defect_count"] / df["sample_size"],
        0.0,
    )
    df["severity"] = df["defect_rate"].apply(_classify_severity)

    df = df.sort_values("inspection_date").reset_index(drop=True)
    return df
