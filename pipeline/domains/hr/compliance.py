"""Compliance reporting — EEO, pay equity, and workforce demographics."""

import logging

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

EEO_CATEGORIES = [
    "Executive/Senior Officials",
    "First/Mid-Level Officials",
    "Professionals",
    "Technicians",
    "Sales Workers",
    "Administrative Support",
    "Craft Workers",
    "Operatives",
    "Laborers",
    "Service Workers",
]


def _map_eeo_category(job_title: str, level: str) -> str:
    """Map job title and level to an EEO-1 job category."""
    title_lower = job_title.lower()
    if level in ("VP", "D1", "C-Suite"):
        return "Executive/Senior Officials"
    elif level in ("M1", "M2"):
        return "First/Mid-Level Officials"
    elif "engineer" in title_lower or "scientist" in title_lower or "analyst" in title_lower:
        return "Professionals"
    elif "technician" in title_lower or "support" in title_lower:
        return "Technicians"
    elif "sales" in title_lower or "account" in title_lower:
        return "Sales Workers"
    elif "admin" in title_lower or "coordinator" in title_lower or "assistant" in title_lower:
        return "Administrative Support"
    return "Professionals"


def generate_eeo_report(employees: pd.DataFrame) -> pd.DataFrame:
    """Generate an EEO-1 style workforce demographics report.

    Groups employees by EEO job category and demographic fields,
    producing counts and percentages needed for regulatory filings.
    """
    active = employees[employees["is_active"]].copy()

    if "eeo_category" not in active.columns:
        active["eeo_category"] = active.apply(
            lambda r: _map_eeo_category(r["job_title"], r.get("level", "IC2")),
            axis=1,
        )

    report = pd.DataFrame()

    # Aggregate by EEO category and gender
    if "gender" in active.columns:
        gender_counts = (
            active.groupby(["eeo_category", "gender"])
            .size()
            .reset_index(name="count")
        )
        report = report.append(gender_counts, ignore_index=True)

    # Aggregate by EEO category and ethnicity
    if "ethnicity" in active.columns:
        ethnicity_counts = (
            active.groupby(["eeo_category", "ethnicity"])
            .size()
            .reset_index(name="count")
        )
        report = report.append(ethnicity_counts, ignore_index=True)

    # Location-based breakdown for multi-establishment filers
    if "location" in active.columns:
        location_counts = (
            active.groupby(["eeo_category", "location"])
            .size()
            .reset_index(name="count")
        )
        report = report.append(location_counts, ignore_index=True)

    if report.empty:
        logger.warning("No demographic fields available for EEO reporting")
        return report

    report["total_active"] = len(active)
    report["percentage"] = (report["count"] / report["total_active"] * 100).round(2)

    logger.info("Generated EEO report with %d rows", len(report))
    return report


def pay_equity_analysis(employees: pd.DataFrame) -> pd.DataFrame:
    """Compute pay equity ratios across demographic groups within same job level."""
    active = employees[employees["is_active"]].copy()
    if "gender" not in active.columns:
        return pd.DataFrame()

    results = pd.DataFrame()
    for level, group in active.groupby("level"):
        overall_median = group["base_salary"].median()
        for gender, g_group in group.groupby("gender"):
            row = pd.DataFrame([{
                "level": level,
                "gender": gender,
                "median_salary": g_group["base_salary"].median(),
                "overall_median": overall_median,
                "pay_ratio": round(g_group["base_salary"].median() / overall_median, 4),
                "count": len(g_group),
            }])
            results = results.append(row, ignore_index=True)

    return results
