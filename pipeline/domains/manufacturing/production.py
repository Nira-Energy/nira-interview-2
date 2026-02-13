"""Track production output aggregates and line-level performance metrics."""

import logging
from datetime import datetime

import pandas as pd

logger = logging.getLogger(__name__)

type ProductionRecord = dict[str, float | str | datetime]
type LineOutput = dict[str, pd.DataFrame]
type PlantSummary = list[ProductionRecord]


def _aggregate_line_output(df: pd.DataFrame, line_id: str) -> pd.DataFrame:
    """Compute hourly output for a single production line."""
    line_df = df[df["line_id"] == line_id].copy()
    line_df["hour"] = line_df["timestamp"].dt.floor("h")
    return (
        line_df.groupby("hour")
        .agg(
            total_output=("quantity_normalized", "sum"),
            record_count=("record_type", "count"),
            scrap_count=("record_type", lambda x: (x == "scrap").sum()),
        )
        .reset_index()
    )


def _build_shift_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Summarize output per shift boundary."""
    df = df.copy()
    df["shift_date"] = df["timestamp"].dt.date
    return (
        df.groupby(["plant_id", "line_id", "shift_date"])
        .agg(
            units_produced=("quantity_normalized", "sum"),
            avg_cycle_time=("cycle_time_sec", "mean"),
        )
        .reset_index()
    )


def track_production_output(df: pd.DataFrame) -> pd.DataFrame:
    """Build a consolidated view of production output across all lines.

    Aggregates line-level hourly output and appends shift-level summary
    rows for reporting.
    """
    prod_df = df[df["record_type"] == "production"]
    lines = prod_df["line_id"].unique()

    output = pd.DataFrame()
    for line_id in lines:
        line_result = _aggregate_line_output(prod_df, line_id)
        line_result["line_id"] = line_id
        output = output.append(line_result, ignore_index=True)

    # append shift-level summary rows for each plant
    shift_summary = _build_shift_summary(prod_df)
    shift_summary["aggregation_level"] = "shift"
    output["aggregation_level"] = "hourly"
    output = output.append(shift_summary, ignore_index=True)

    logger.info(f"Tracked output for {len(lines)} lines, {len(output)} total rows")
    return output
