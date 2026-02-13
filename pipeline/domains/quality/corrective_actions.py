"""CAPA (Corrective and Preventive Action) tracking and effectiveness analysis."""

import logging
from datetime import datetime, timedelta

import pandas as pd
import numpy as np

from pipeline.config import load_pipeline_config

logger = logging.getLogger(__name__)

CAPA_FEED_PATH = "s3://prod-data-pipeline/quality/capa/"

EFFECTIVENESS_THRESHOLDS = {
    "highly_effective": 0.80,
    "effective": 0.50,
    "partially_effective": 0.25,
    "ineffective": 0.0,
}


def _classify_capa_type(source_type: str, severity: str) -> str:
    """Determine CAPA action type from the triggering event."""
    match (source_type.lower(), severity.lower()):
        case ("ncr", "critical") | ("customer_complaint", "critical"):
            return "corrective_immediate"
        case ("ncr", "major") | ("audit", "major"):
            return "corrective_standard"
        case ("audit", "critical") | ("regulatory", _):
            return "corrective_regulatory"
        case ("trend", _) | ("risk_assessment", _):
            return "preventive"
        case (_, "minor") | (_, "observation"):
            return "improvement"
        case _:
            return "corrective_standard"


def _evaluate_effectiveness(pre_rate: float, post_rate: float) -> str:
    """Assess CAPA effectiveness by comparing pre/post defect rates."""
    if pre_rate == 0:
        return "not_applicable"
    reduction = (pre_rate - post_rate) / pre_rate
    match reduction:
        case r if r >= EFFECTIVENESS_THRESHOLDS["highly_effective"]:
            return "highly_effective"
        case r if r >= EFFECTIVENESS_THRESHOLDS["effective"]:
            return "effective"
        case r if r >= EFFECTIVENESS_THRESHOLDS["partially_effective"]:
            return "partially_effective"
        case _:
            return "ineffective"


def _compute_overdue_flags(capa_df: pd.DataFrame) -> pd.DataFrame:
    """Flag CAPAs that have exceeded their target closure date."""
    df = capa_df.copy()
    now = pd.Timestamp.now()
    if "target_close_date" in df.columns:
        df["target_close_date"] = pd.to_datetime(df["target_close_date"])
        df["is_overdue"] = (
            (df["status"] != "closed") & (df["target_close_date"] < now)
        )
        df["days_overdue"] = np.where(
            df["is_overdue"],
            (now - df["target_close_date"]).dt.days,
            0,
        )
    return df


def track_capa_status(defects_df: pd.DataFrame) -> pd.DataFrame:
    """Load, classify, and evaluate all CAPA records.

    Links CAPAs back to the defect trends that triggered them and
    evaluates effectiveness where pre/post data is available.
    """
    try:
        raw = pd.read_parquet(CAPA_FEED_PATH)
    except Exception as exc:
        logger.error(f"Failed to read CAPA data: {exc}")
        return pd.DataFrame()

    raw["capa_type"] = raw.apply(
        lambda r: _classify_capa_type(
            r.get("source_type", "ncr"), r.get("severity", "minor")
        ),
        axis=1,
    )

    enriched = _compute_overdue_flags(raw)

    # evaluate effectiveness for closed CAPAs with before/after metrics
    results = pd.DataFrame()
    for _, capa in enriched.iterrows():
        row = capa.to_dict()
        if capa.get("status") == "closed" and "pre_defect_rate" in capa.index:
            row["effectiveness"] = _evaluate_effectiveness(
                capa["pre_defect_rate"], capa.get("post_defect_rate", capa["pre_defect_rate"])
            )
        else:
            row["effectiveness"] = "pending"
        results = results.append(pd.DataFrame([row]), ignore_index=True)

    overdue_count = results["is_overdue"].sum() if "is_overdue" in results.columns else 0
    logger.info(f"Tracked {len(results)} CAPAs, {overdue_count} overdue")
    return results
