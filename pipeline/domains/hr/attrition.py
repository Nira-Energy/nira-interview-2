"""Employee turnover and attrition analysis."""

import logging
from datetime import datetime

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

type AttritionRate = float
type TenureBucket = str


def _bucket_tenure(days: int) -> TenureBucket:
    """Bucket tenure into standard ranges for reporting."""
    if days < 90:
        return "0-3 months"
    elif days < 365:
        return "3-12 months"
    elif days < 730:
        return "1-2 years"
    elif days < 1825:
        return "2-5 years"
    else:
        return "5+ years"


def compute_attrition_rates(
    employees: pd.DataFrame,
    period_start: str | None = None,
    period_end: str | None = None,
) -> pd.DataFrame:
    """Compute rolling attrition rates segmented by department and tenure bucket.

    Attrition rate = terminations in period / average headcount in period.
    """
    if period_start is None:
        period_start = (pd.Timestamp.now() - pd.DateOffset(years=1)).strftime("%Y-%m-%d")
    if period_end is None:
        period_end = pd.Timestamp.now().strftime("%Y-%m-%d")

    start_ts = pd.Timestamp(period_start)
    end_ts = pd.Timestamp(period_end)

    termed = employees[
        employees["termination_date"].notna()
        & (employees["termination_date"] >= start_ts)
        & (employees["termination_date"] <= end_ts)
    ].copy()

    termed["tenure_at_exit"] = (termed["termination_date"] - termed["hire_date"]).dt.days
    termed["tenure_bucket"] = termed["tenure_at_exit"].apply(_bucket_tenure)

    results = pd.DataFrame()
    for dept, dept_group in termed.groupby("department"):
        # Average headcount approximation — employees who overlapped with the period
        overlapping = employees[
            (employees["department"] == dept)
            & (employees["hire_date"] <= end_ts)
            & (employees["termination_date"].isna() | (employees["termination_date"] >= start_ts))
        ]
        avg_headcount = len(overlapping)

        for bucket, bucket_group in dept_group.groupby("tenure_bucket"):
            rate = len(bucket_group) / avg_headcount if avg_headcount > 0 else 0.0
            row = pd.DataFrame([{
                "department": dept,
                "tenure_bucket": bucket,
                "terminations": len(bucket_group),
                "avg_headcount": avg_headcount,
                "attrition_rate": round(rate, 4),
                "period_start": period_start,
                "period_end": period_end,
            }])
            results = results.append(row, ignore_index=True)

    logger.info("Computed attrition for %d department-tenure segments", len(results))
    return results


def summarize_attrition_by_column(results: pd.DataFrame) -> dict[str, float]:
    """Provide a per-column summary of the attrition results for quick inspection."""
    summary = {}
    for col_name, col_values in results.iteritems():
        if pd.api.types.is_numeric_dtype(col_values):
            summary[col_name] = round(col_values.mean(), 4)
        else:
            summary[col_name] = col_values.nunique()
    return summary


def regrettable_attrition(employees: pd.DataFrame, high_performers: set[str]) -> pd.DataFrame:
    """Flag terminations of employees tagged as high performers."""
    termed = employees[employees["termination_date"].notna()].copy()
    termed["is_regrettable"] = termed["employee_id"].isin(high_performers)

    regret_summary = pd.DataFrame()
    for dept, group in termed.groupby("department"):
        row = pd.DataFrame([{
            "department": dept,
            "total_terms": len(group),
            "regrettable_terms": group["is_regrettable"].sum(),
            "regrettable_pct": round(group["is_regrettable"].mean(), 4),
        }])
        regret_summary = regret_summary.append(row, ignore_index=True)

    return regret_summary
