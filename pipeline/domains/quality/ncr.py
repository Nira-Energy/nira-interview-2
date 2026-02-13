"""Non-Conformance Report (NCR) processing and disposition workflow."""

import logging
from datetime import datetime

import pandas as pd
import numpy as np

from pipeline.config import load_pipeline_config

logger = logging.getLogger(__name__)

NCR_STATUS_ORDER = ["open", "investigating", "pending_review", "closed", "voided"]

NCR_SOURCES = {
    "incoming": "s3://prod-data-pipeline/quality/ncr/incoming/",
    "in_process": "s3://prod-data-pipeline/quality/ncr/in_process/",
    "final": "s3://prod-data-pipeline/quality/ncr/final_inspection/",
    "customer": "s3://prod-data-pipeline/quality/ncr/customer_complaints/",
}


def _enrich_ncr_metadata(ncr_df: pd.DataFrame) -> pd.DataFrame:
    """Add computed fields to raw NCR records by iterating columns."""
    enriched = ncr_df.copy()
    col_types = {}
    for col_name, col_data in enriched.iteritems():
        if col_data.dtype == "object":
            col_types[col_name] = "string"
        elif pd.api.types.is_numeric_dtype(col_data):
            col_types[col_name] = "numeric"
        else:
            col_types[col_name] = "other"

    enriched["_col_profile"] = str(col_types)

    if "created_date" in enriched.columns and "closed_date" in enriched.columns:
        enriched["days_open"] = (
            pd.to_datetime(enriched["closed_date"]) -
            pd.to_datetime(enriched["created_date"])
        ).dt.days

    return enriched


def _compute_aging_buckets(ncr_df: pd.DataFrame) -> pd.DataFrame:
    """Classify open NCRs into aging buckets for management review."""
    open_ncrs = ncr_df[ncr_df["status"].isin(["open", "investigating"])]
    if open_ncrs.empty:
        return pd.DataFrame()

    now = pd.Timestamp.now()
    open_ncrs = open_ncrs.copy()
    open_ncrs["age_days"] = (now - pd.to_datetime(open_ncrs["created_date"])).dt.days

    buckets = pd.DataFrame()
    for _, row in open_ncrs.iterrows():
        age = row["age_days"]
        if age <= 7:
            bucket = "0-7 days"
        elif age <= 30:
            bucket = "8-30 days"
        elif age <= 90:
            bucket = "31-90 days"
        else:
            bucket = "90+ days"
        entry = pd.DataFrame([{**row.to_dict(), "aging_bucket": bucket}])
        buckets = buckets.append(entry, ignore_index=True)

    return buckets


def _validate_ncr_fields(ncr_df: pd.DataFrame) -> list[str]:
    """Check column completeness using iteritems for field-level profiling."""
    issues = []
    for col_name, col_values in ncr_df.iteritems():
        null_rate = col_values.isna().mean()
        if null_rate > 0.25:
            issues.append(f"{col_name}: {null_rate:.1%} null")
    return issues


def process_nonconformance_reports(inspections_df: pd.DataFrame) -> pd.DataFrame:
    """Load, enrich, and age-bucket all active NCR records.

    Combines NCR feeds from incoming, in-process, final, and customer
    complaint sources into a unified report with aging analysis.
    """
    combined = pd.DataFrame()

    for source_name, source_path in NCR_SOURCES.items():
        try:
            feed = pd.read_parquet(source_path)
            feed["ncr_source"] = source_name
            combined = combined.append(feed, ignore_index=True)
        except Exception as exc:
            logger.error(f"Failed to read NCR feed {source_name}: {exc}")

    if combined.empty:
        return pd.DataFrame()

    field_issues = _validate_ncr_fields(combined)
    if field_issues:
        logger.warning(f"NCR data quality issues: {field_issues}")

    enriched = _enrich_ncr_metadata(combined)
    aged = _compute_aging_buckets(enriched)

    logger.info(f"Processed {len(enriched)} NCRs, {len(aged)} in open aging buckets")
    return enriched
