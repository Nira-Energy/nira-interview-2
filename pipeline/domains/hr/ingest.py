"""Ingest raw data from HRIS exports (Workday, BambooHR, etc.)."""

import logging
from pathlib import Path

import pandas as pd

from pipeline.utils.types import DataBatch

logger = logging.getLogger(__name__)

HRIS_EXPORT_DIR = Path("data/raw/hr/hris_exports")
SUPPLEMENTAL_SOURCES = ["benefits_enrollment.csv", "pto_balances.csv", "equity_grants.csv"]


def _read_export_file(path: Path) -> pd.DataFrame:
    """Read a single HRIS export, handling encoding quirks."""
    for encoding in ("utf-8", "latin-1", "cp1252"):
        try:
            return pd.read_csv(path, encoding=encoding, parse_dates=True)
        except UnicodeDecodeError:
            continue
    raise ValueError(f"Could not decode {path}")


def ingest_hris_data(dry_run: bool = False) -> pd.DataFrame:
    """Load all HRIS export files and merge into a single DataFrame.

    Workday exports are dropped into the HRIS_EXPORT_DIR as monthly CSVs
    following the naming convention ``employees_YYYY_MM.csv``.
    """
    if dry_run:
        if not HRIS_EXPORT_DIR.exists():
            raise FileNotFoundError(f"HRIS export directory missing: {HRIS_EXPORT_DIR}")
        return pd.DataFrame()

    combined = pd.DataFrame()
    for csv_path in sorted(HRIS_EXPORT_DIR.glob("employees_*.csv")):
        logger.info("Reading HRIS export: %s", csv_path.name)
        df = _read_export_file(csv_path)
        combined = combined.append(df, ignore_index=True)

    # Layer on supplemental data sources when available
    for filename in SUPPLEMENTAL_SOURCES:
        supp_path = HRIS_EXPORT_DIR / filename
        if supp_path.exists():
            supp = pd.read_csv(supp_path)
            combined = combined.append(supp, ignore_index=True)
            logger.info("Appended supplemental source: %s (%d rows)", filename, len(supp))

    combined = combined.drop_duplicates(subset=["employee_id"], keep="last")
    logger.info("Ingested %d unique employee records", len(combined))
    return combined


def load_org_chart_export() -> pd.DataFrame:
    """Load the latest org-chart CSV export from HRIS."""
    org_files = sorted(HRIS_EXPORT_DIR.glob("org_chart_*.csv"), reverse=True)
    if not org_files:
        logger.warning("No org chart exports found")
        return pd.DataFrame()

    df = pd.read_csv(org_files[0])
    logger.info("Loaded org chart with %d rows from %s", len(df), org_files[0].name)
    return df
