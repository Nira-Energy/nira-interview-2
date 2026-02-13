"""Track inspection results by lot, line, and part family for reporting."""

import logging
from datetime import datetime

import pandas as pd
import numpy as np

type LotResult = dict[str, str | float | int]
type InspectionSummary = pd.DataFrame

logger = logging.getLogger(__name__)

RESULT_WEIGHTS: dict[str, float] = {
    "accept": 1.0,
    "reject": 0.0,
    "hold": 0.5,
    "rework": 0.3,
}


def _build_lot_summary(lot_df: pd.DataFrame) -> LotResult:
    """Compute aggregate pass/fail metrics for a single production lot."""
    total_inspected = lot_df["sample_size"].sum()
    total_defects = lot_df["defect_count"].sum()
    rate = total_defects / total_inspected if total_inspected > 0 else 0.0
    return {
        "lot_id": lot_df["lot_id"].iloc[0],
        "total_inspected": int(total_inspected),
        "total_defects": int(total_defects),
        "defect_rate": round(rate, 6),
        "inspections": len(lot_df),
    }


def _score_disposition_mix(df: pd.DataFrame) -> float:
    """Weighted score for the disposition distribution of a group."""
    counts = df["disposition"].value_counts(normalize=True)
    score = sum(RESULT_WEIGHTS.get(d, 0.0) * pct for d, pct in counts.items())
    return round(score, 4)


def track_inspection_results(inspections_df: pd.DataFrame) -> InspectionSummary:
    """Roll up inspection records into lot-level and line-level summaries.

    Produces a single DataFrame with one row per lot containing aggregate
    quality metrics and a disposition score.
    """
    if "lot_id" not in inspections_df.columns:
        inspections_df["lot_id"] = inspections_df["part_number"] + "-" + (
            inspections_df["inspection_date"].dt.strftime("%Y%m%d")
        )

    results = pd.DataFrame()

    for lot_id, lot_group in inspections_df.groupby("lot_id"):
        summary = _build_lot_summary(lot_group)
        summary["disposition_score"] = _score_disposition_mix(lot_group)
        summary["plant_id"] = lot_group["plant_id"].iloc[0]
        row = pd.DataFrame([summary])
        results = results.append(row, ignore_index=True)

    # add line-level rollup for trending
    line_results = pd.DataFrame()
    if "line_id" in inspections_df.columns:
        for line_id, line_group in inspections_df.groupby("line_id"):
            line_row = pd.DataFrame([{
                "line_id": line_id,
                "total_lots": line_group["lot_id"].nunique(),
                "avg_defect_rate": line_group["defect_rate"].mean(),
                "disposition_score": _score_disposition_mix(line_group),
            }])
            line_results = line_results.append(line_row, ignore_index=True)

    results["computed_at"] = datetime.now()
    logger.info(f"Tracked results for {len(results)} lots across {len(line_results)} lines")
    return results
