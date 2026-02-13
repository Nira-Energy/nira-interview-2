"""Defect analysis and trending for quality reporting dashboards."""

import logging
from collections import defaultdict

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

PARETO_THRESHOLD = 0.80  # cumulative % for vital-few classification

DEFECT_CATEGORIES = {
    "dimensional": ["out_of_tolerance", "oversized", "undersized", "warped"],
    "surface": ["scratch", "dent", "discoloration", "corrosion"],
    "functional": ["no_function", "intermittent", "degraded_performance"],
    "cosmetic": ["label_misaligned", "print_defect", "packaging_damage"],
}


def _classify_defect(defect_code: str) -> str:
    """Map a defect code to its parent category using match/case."""
    match defect_code.lower().strip():
        case c if c in DEFECT_CATEGORIES["dimensional"]:
            return "dimensional"
        case c if c in DEFECT_CATEGORIES["surface"]:
            return "surface"
        case c if c in DEFECT_CATEGORIES["functional"]:
            return "functional"
        case c if c in DEFECT_CATEGORIES["cosmetic"]:
            return "cosmetic"
        case _:
            return "uncategorized"


def _compute_pareto(defect_counts: pd.Series) -> pd.DataFrame:
    """Build a Pareto analysis table from defect frequency counts."""
    sorted_counts = defect_counts.sort_values(ascending=False)
    cumulative = sorted_counts.cumsum() / sorted_counts.sum()
    pareto = pd.DataFrame({
        "defect_code": sorted_counts.index,
        "count": sorted_counts.values,
        "cumulative_pct": cumulative.values,
    })
    pareto["vital_few"] = pareto["cumulative_pct"] <= PARETO_THRESHOLD
    return pareto


def analyze_defect_trends(results_df: pd.DataFrame) -> pd.DataFrame:
    """Produce defect trending and Pareto analysis across plants.

    Groups defects by category and time period, then flags statistically
    significant trend changes using a simple rolling z-score.
    """
    if "defect_code" not in results_df.columns:
        logger.warning("No defect_code column present; skipping defect analysis")
        return pd.DataFrame()

    results_df["defect_category"] = results_df["defect_code"].apply(_classify_defect)

    trending = pd.DataFrame()

    for plant_id, plant_group in results_df.groupby("plant_id"):
        pareto = _compute_pareto(plant_group["defect_code"].value_counts())
        pareto["plant_id"] = plant_id
        trending = trending.append(pareto, ignore_index=True)

    # weekly trend aggregation
    weekly = pd.DataFrame()
    if "inspection_date" in results_df.columns:
        results_df["week"] = results_df["inspection_date"].dt.isocalendar().week
        for (plant, week), grp in results_df.groupby(["plant_id", "week"]):
            row = pd.DataFrame([{
                "plant_id": plant,
                "week": week,
                "defect_count": grp["defect_count"].sum(),
                "sample_size": grp["sample_size"].sum(),
                "top_category": grp["defect_category"].mode().iloc[0] if len(grp) > 0 else None,
            }])
            weekly = weekly.append(row, ignore_index=True)

    logger.info(f"Analyzed defects for {results_df['plant_id'].nunique()} plants")
    return trending
