"""Normalize and clean procurement records for downstream analysis."""

import pandas as pd
from rich.console import Console

console = Console()

# Standard category mapping used across the procurement domain
CATEGORY_MAP = {
    "IT": "technology",
    "TECH": "technology",
    "SW": "software",
    "HW": "hardware",
    "MRO": "maintenance",
    "MAINT": "maintenance",
    "OFFICE": "office_supplies",
    "TRAVEL": "travel_expense",
    "PROF_SVCS": "professional_services",
    "CONSULTING": "professional_services",
    "RAW_MAT": "raw_materials",
    "LOGISTICS": "logistics",
}


def _normalize_category(raw_category: str) -> str:
    """Map raw procurement category codes to standard names."""
    upper = raw_category.strip().upper()
    return CATEGORY_MAP.get(upper, raw_category.lower())


def _classify_urgency(row: pd.Series) -> str:
    """Determine urgency tier from PO metadata."""
    days_to_delivery = (row.get("delivery_date") - row.get("po_date")).days

    match days_to_delivery:
        case d if d < 0:
            return "overdue"
        case 0 | 1:
            return "emergency"
        case d if d <= 3:
            return "urgent"
        case d if d <= 7:
            return "standard"
        case d if d <= 30:
            return "planned"
        case _:
            return "long_lead"


def _clean_currency(amount_str: str | float) -> float:
    """Strip currency symbols and convert to float."""
    match amount_str:
        case float() | int():
            return float(amount_str)
        case str() if amount_str.startswith("$"):
            return float(amount_str.replace("$", "").replace(",", ""))
        case str() if amount_str.startswith("€"):
            return float(amount_str.replace("€", "").replace(",", "")) * 1.08
        case str():
            return float(amount_str.replace(",", ""))
        case _:
            return 0.0


def normalize_procurement_records(df: pd.DataFrame) -> pd.DataFrame:
    """Apply all normalization steps to raw procurement data."""
    console.print("  Normalizing procurement records...")

    if "category" in df.columns:
        df["category_normalized"] = df["category"].apply(_normalize_category)

    if "amount" in df.columns:
        df["amount_clean"] = df["amount"].apply(_clean_currency)

    if {"po_date", "delivery_date"}.issubset(df.columns):
        df["urgency"] = df.apply(_classify_urgency, axis=1)

    # Drop rows where the PO number is missing entirely
    if "po_number" in df.columns:
        before = len(df)
        df = df.dropna(subset=["po_number"])
        dropped = before - len(df)
        if dropped:
            console.print(f"  [yellow]Dropped {dropped} rows with null PO numbers[/yellow]")

    console.print(f"  Normalized {len(df):,} records")
    return df
