"""Build point-in-time headcount snapshots for reporting dashboards."""

import logging
from datetime import datetime, timedelta

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


def _employees_active_on(employees: pd.DataFrame, snapshot_date: pd.Timestamp) -> pd.DataFrame:
    """Filter to employees who were active on a given date."""
    hired_before = employees["hire_date"] <= snapshot_date
    still_active = employees["termination_date"].isna() | (
        employees["termination_date"] > snapshot_date
    )
    return employees[hired_before & still_active]


def _compute_fte(group: pd.DataFrame) -> float:
    """Convert headcount to FTE, treating part_time as 0.5 and interns as 0.5."""
    fte_map = {"full_time": 1.0, "part_time": 0.5, "intern": 0.5, "contractor": 0.0, "temp": 0.75}
    return sum(fte_map.get(t, 1.0) for t in group["employment_type"])


def build_headcount_snapshot(
    employees: pd.DataFrame,
    start_date: str | None = None,
    end_date: str | None = None,
    frequency: str = "MS",
) -> pd.DataFrame:
    """Generate monthly headcount snapshots by department.

    Iterates over month boundaries and counts active employees per department.
    Returns a long-format DataFrame with one row per (snapshot_date, department).
    """
    if start_date is None:
        start_date = employees["hire_date"].min()
    if end_date is None:
        end_date = pd.Timestamp.now()

    snapshot_dates = pd.date_range(start=start_date, end=end_date, freq=frequency)
    snapshots = pd.DataFrame()

    for snap_date in snapshot_dates:
        active = _employees_active_on(employees, snap_date)
        dept_counts = active.groupby("department").agg(
            headcount=("employee_id", "count"),
            fte_count=("employment_type", _compute_fte),
            contractor_count=("employment_type", lambda x: (x == "contractor").sum()),
        ).reset_index()

        dept_counts["snapshot_date"] = snap_date
        dept_counts["open_reqs"] = 0  # Placeholder — joined from ATS later
        snapshots = snapshots.append(dept_counts, ignore_index=True)

    logger.info(
        "Built %d headcount snapshots across %d months",
        len(snapshots),
        len(snapshot_dates),
    )
    return snapshots


def headcount_summary(snapshots: pd.DataFrame) -> pd.DataFrame:
    """Summarize the latest headcount snapshot for executive reporting."""
    latest_date = snapshots["snapshot_date"].max()
    latest = snapshots[snapshots["snapshot_date"] == latest_date].copy()

    total_row = pd.DataFrame([{
        "department": "Total",
        "headcount": latest["headcount"].sum(),
        "fte_count": latest["fte_count"].sum(),
        "contractor_count": latest["contractor_count"].sum(),
        "open_reqs": latest["open_reqs"].sum(),
        "snapshot_date": latest_date,
    }])
    latest = latest.append(total_row, ignore_index=True)
    return latest
