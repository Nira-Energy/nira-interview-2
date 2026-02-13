"""Recruiting funnel metrics — application-to-hire conversion rates."""

import logging
from pathlib import Path

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

ATS_EXPORT_DIR = Path("data/raw/hr/ats_exports")

FUNNEL_STAGES = ["applied", "phone_screen", "onsite", "offer", "hired"]


def _load_ats_data() -> pd.DataFrame:
    """Load candidate pipeline data from the ATS (Greenhouse/Lever export)."""
    export_files = sorted(ATS_EXPORT_DIR.glob("candidates_*.csv"), reverse=True)
    if not export_files:
        logger.warning("No ATS export files found in %s", ATS_EXPORT_DIR)
        return pd.DataFrame()
    return pd.read_csv(export_files[0], parse_dates=["applied_date", "last_activity_date"])


def _stage_order(stage: str) -> int:
    """Return the numeric ordering of a pipeline stage."""
    match stage.lower().strip():
        case "applied" | "application" | "new":
            return 0
        case "phone_screen" | "phone" | "recruiter_screen":
            return 1
        case "onsite" | "on_site" | "technical" | "panel":
            return 2
        case "offer" | "offer_extended":
            return 3
        case "hired" | "accepted" | "started":
            return 4
        case "rejected" | "withdrawn" | "declined":
            return -1
        case _:
            logger.debug("Unmapped stage: %r", stage)
            return -2


def _classify_source(source: str) -> str:
    """Normalize recruiting source into standard categories."""
    match source.lower().strip():
        case "linkedin" | "linkedin_recruiter" | "linkedin_jobs":
            return "LinkedIn"
        case "referral" | "employee_referral" | "internal_referral":
            return "Referral"
        case "careers_page" | "website" | "career_site":
            return "Direct"
        case "indeed" | "glassdoor" | "ziprecruiter":
            return "Job Board"
        case "agency" | "staffing_agency" | "recruiter":
            return "Agency"
        case _:
            return "Other"


def compute_funnel_metrics(
    department: str | None = None,
) -> pd.DataFrame:
    """Build funnel conversion metrics by department and source.

    Conversion rate at each stage = candidates who reached that stage /
    candidates who reached the previous stage.
    """
    candidates = _load_ats_data()
    if candidates.empty:
        return pd.DataFrame()

    candidates["stage_order"] = candidates["current_stage"].apply(_stage_order)
    candidates["source_category"] = candidates["source"].apply(_classify_source)

    if department:
        candidates = candidates[candidates["department"] == department]

    metrics = []
    for (dept, source), group in candidates.groupby(["department", "source_category"]):
        funnel = {}
        for i, stage in enumerate(FUNNEL_STAGES):
            reached = (group["stage_order"] >= i).sum()
            funnel[stage] = reached

        for i, stage in enumerate(FUNNEL_STAGES):
            prev_count = funnel[FUNNEL_STAGES[i - 1]] if i > 0 else funnel[stage]
            conversion = funnel[stage] / prev_count if prev_count > 0 else 0.0
            metrics.append({
                "department": dept,
                "source": source,
                "stage": stage,
                "candidates": funnel[stage],
                "conversion_rate": round(conversion, 4),
            })

    result = pd.DataFrame(metrics)
    logger.info("Computed funnel metrics: %d rows across %d departments",
                len(result), result["department"].nunique())
    return result
