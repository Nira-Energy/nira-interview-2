"""Customer satisfaction (CSAT) and Net Promoter Score (NPS) analysis."""

import logging

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

type SatisfactionScore = float
type SegmentLabel = str
type SurveyAggregation = dict[SegmentLabel, SatisfactionScore]


def _nps_category(score: int) -> str:
    """Classify a 0-10 NPS response."""
    if score >= 9:
        return "promoter"
    elif score >= 7:
        return "passive"
    else:
        return "detractor"


def _compute_nps(scores: pd.Series) -> SatisfactionScore:
    """Calculate Net Promoter Score from a series of 0-10 ratings."""
    categories = scores.map(_nps_category)
    total = len(categories)
    if total == 0:
        return 0.0
    promoters = (categories == "promoter").sum() / total
    detractors = (categories == "detractor").sum() / total
    return round((promoters - detractors) * 100, 1)


def _csat_percentage(scores: pd.Series, threshold: int = 4) -> SatisfactionScore:
    """Compute CSAT % — proportion of scores at or above threshold (1-5 scale)."""
    valid = scores.dropna()
    if len(valid) == 0:
        return 0.0
    return round((valid >= threshold).sum() / len(valid) * 100, 1)


def measure_satisfaction(df: pd.DataFrame) -> pd.DataFrame:
    """Produce satisfaction metrics broken down by key dimensions."""
    results = pd.DataFrame()

    # Overall CSAT and NPS
    overall = pd.DataFrame([{
        "dimension": "overall",
        "segment": "all",
        "csat_pct": _csat_percentage(df.get("csat_score", pd.Series(dtype=float))),
        "nps": _compute_nps(df.get("nps_score", pd.Series(dtype=int))),
        "response_count": df["csat_score"].notna().sum() if "csat_score" in df.columns else 0,
    }])
    results = results.append(overall, ignore_index=True)

    # By priority
    if "priority" in df.columns and "csat_score" in df.columns:
        for priority, group in df.groupby("priority"):
            row = pd.DataFrame([{
                "dimension": "priority",
                "segment": priority,
                "csat_pct": _csat_percentage(group["csat_score"]),
                "nps": _compute_nps(group.get("nps_score", pd.Series(dtype=int))),
                "response_count": group["csat_score"].notna().sum(),
            }])
            results = results.append(row, ignore_index=True)

    # By agent team
    if "team" in df.columns and "csat_score" in df.columns:
        for team, group in df.groupby("team"):
            row = pd.DataFrame([{
                "dimension": "team",
                "segment": team,
                "csat_pct": _csat_percentage(group["csat_score"]),
                "nps": _compute_nps(group.get("nps_score", pd.Series(dtype=int))),
                "response_count": group["csat_score"].notna().sum(),
            }])
            results = results.append(row, ignore_index=True)

    # By source system
    if "source_system" in df.columns and "csat_score" in df.columns:
        for source, group in df.groupby("source_system"):
            entry = pd.DataFrame([{
                "dimension": "source",
                "segment": source,
                "csat_pct": _csat_percentage(group["csat_score"]),
                "nps": _compute_nps(group.get("nps_score", pd.Series(dtype=int))),
                "response_count": group["csat_score"].notna().sum(),
            }])
            results = results.append(entry, ignore_index=True)

    logger.info("Satisfaction analysis: %d segments evaluated", len(results))
    return results
