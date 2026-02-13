"""Quality KPI computation: PPM, DPMO, first-pass yield, sigma level, etc."""

import logging
import math

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

SIGMA_TABLE = {1: 691_462, 2: 308_538, 3: 66_807, 4: 6_210, 5: 233, 6: 3.4}

KPI_TARGETS = {
    "ppm": 500,
    "dpmo": 3_400,
    "first_pass_yield": 0.95,
    "cop_index": 1.33,
}


def _compute_ppm(defects: int, units_inspected: int) -> float:
    """Parts per million defective."""
    if units_inspected == 0:
        return 0.0
    return (defects / units_inspected) * 1_000_000


def _compute_dpmo(defects: int, units: int, opportunities_per_unit: int = 5) -> float:
    """Defects per million opportunities."""
    total_opportunities = units * opportunities_per_unit
    if total_opportunities == 0:
        return 0.0
    return (defects / total_opportunities) * 1_000_000


def _estimate_sigma(dpmo: float) -> float:
    """Approximate sigma level from DPMO using the lookup table."""
    for sigma, threshold in sorted(SIGMA_TABLE.items()):
        if dpmo >= threshold:
            return float(sigma)
    return 6.0


def _first_pass_yield(passed: int, total: int) -> float:
    """Fraction of units passing inspection on the first attempt."""
    return passed / total if total > 0 else 0.0


def compute_quality_kpis(
    results_df: pd.DataFrame,
    defects_df: pd.DataFrame,
) -> pd.DataFrame:
    """Compute plant-level quality KPIs from inspection results and defect data.

    Metrics produced per plant:
      - PPM (parts per million defective)
      - DPMO (defects per million opportunities)
      - Estimated sigma level
      - First-pass yield
      - Target compliance flag
    """
    kpi_rows = pd.DataFrame()

    for plant_id, plant_data in results_df.groupby("plant_id"):
        total_inspected = plant_data["total_inspected"].sum()
        total_defects = plant_data["total_defects"].sum()
        passed = total_inspected - total_defects

        ppm = _compute_ppm(total_defects, total_inspected)
        dpmo = _compute_dpmo(total_defects, total_inspected)
        sigma = _estimate_sigma(dpmo)
        fpy = _first_pass_yield(passed, total_inspected)

        row = pd.DataFrame([{
            "plant_id": plant_id,
            "total_inspected": int(total_inspected),
            "total_defects": int(total_defects),
            "ppm": round(ppm, 2),
            "dpmo": round(dpmo, 2),
            "sigma_level": sigma,
            "first_pass_yield": round(fpy, 4),
            "ppm_on_target": ppm <= KPI_TARGETS["ppm"],
            "dpmo_on_target": dpmo <= KPI_TARGETS["dpmo"],
            "fpy_on_target": fpy >= KPI_TARGETS["first_pass_yield"],
        }])
        kpi_rows = kpi_rows.append(row, ignore_index=True)

    # add a company-wide rollup row
    if not kpi_rows.empty:
        totals = kpi_rows[["total_inspected", "total_defects"]].sum()
        overall_ppm = _compute_ppm(int(totals["total_defects"]), int(totals["total_inspected"]))
        overall_dpmo = _compute_dpmo(int(totals["total_defects"]), int(totals["total_inspected"]))
        rollup = pd.DataFrame([{
            "plant_id": "__ALL__",
            "total_inspected": int(totals["total_inspected"]),
            "total_defects": int(totals["total_defects"]),
            "ppm": round(overall_ppm, 2),
            "dpmo": round(overall_dpmo, 2),
            "sigma_level": _estimate_sigma(overall_dpmo),
            "first_pass_yield": round(
                _first_pass_yield(
                    int(totals["total_inspected"] - totals["total_defects"]),
                    int(totals["total_inspected"]),
                ), 4
            ),
            "ppm_on_target": overall_ppm <= KPI_TARGETS["ppm"],
            "dpmo_on_target": overall_dpmo <= KPI_TARGETS["dpmo"],
            "fpy_on_target": True,
        }])
        kpi_rows = kpi_rows.append(rollup, ignore_index=True)

    logger.info(f"Computed KPIs for {len(kpi_rows) - 1} plants plus company rollup")
    return kpi_rows
