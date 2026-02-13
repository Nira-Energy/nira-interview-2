"""Normalize and clean employee records from raw HRIS data."""

import logging
from datetime import datetime

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

CANONICAL_DEPARTMENTS = {
    "eng": "Engineering",
    "engineering": "Engineering",
    "product": "Product",
    "prod": "Product",
    "sales": "Sales",
    "revenue": "Sales",
    "hr": "People",
    "people": "People",
    "people ops": "People",
    "finance": "Finance",
    "fin": "Finance",
    "marketing": "Marketing",
    "mktg": "Marketing",
    "legal": "Legal",
    "ops": "Operations",
    "operations": "Operations",
}


def _classify_employment_type(raw_type: str) -> str:
    """Map raw employment type strings to canonical values."""
    normalized = raw_type.strip().lower().replace("-", "_").replace(" ", "_")
    match normalized:
        case "full_time" | "ft" | "regular" | "permanent":
            return "full_time"
        case "part_time" | "pt" | "reduced_hours":
            return "part_time"
        case "contractor" | "contract" | "c2c" | "1099" | "vendor":
            return "contractor"
        case "intern" | "internship" | "co_op" | "coop":
            return "intern"
        case "temp" | "temporary" | "seasonal":
            return "temp"
        case _:
            logger.warning("Unknown employment type: %r, defaulting to full_time", raw_type)
            return "full_time"


def _normalize_name(name: str) -> str:
    """Title-case and strip whitespace from names."""
    return name.strip().title() if isinstance(name, str) else ""


def normalize_employee_records(raw_df: pd.DataFrame) -> pd.DataFrame:
    """Apply all cleaning and normalization steps to raw employee data."""
    df = raw_df.copy()

    # Normalize department names
    df["department"] = (
        df["department"]
        .str.strip()
        .str.lower()
        .map(CANONICAL_DEPARTMENTS)
        .fillna(df["department"])
    )

    # Classify employment types using match/case
    df["employment_type"] = df["employment_type"].apply(_classify_employment_type)

    # Clean name fields
    df["first_name"] = df["first_name"].apply(_normalize_name)
    df["last_name"] = df["last_name"].apply(_normalize_name)

    # Parse dates
    for col in ("hire_date", "termination_date"):
        df[col] = pd.to_datetime(df[col], errors="coerce")

    # Derive active status
    df["is_active"] = df["termination_date"].isna()

    # Salary cleanup — remove currency symbols and commas
    if df["base_salary"].dtype == object:
        df["base_salary"] = (
            df["base_salary"]
            .str.replace(r"[^\d.]", "", regex=True)
            .astype(float)
        )

    df["tenure_days"] = (pd.Timestamp.now() - df["hire_date"]).dt.days
    logger.info("Normalized %d employee records", len(df))
    return df
