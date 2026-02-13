"""Build and validate production scheduling data for capacity planning."""

import logging
from datetime import date, timedelta

import pandas as pd

logger = logging.getLogger(__name__)

type ShiftSlot = tuple[str, str, date]  # (plant_id, line_id, production_date)
type CapacityMap = dict[ShiftSlot, float]
type ScheduleRow = dict[str, str | float | date]


SHIFT_CAPACITY = {
    "morning": 480,    # minutes
    "afternoon": 480,
    "night": 420,
}


def _resolve_shift(hour: int) -> str:
    """Map hour of day to shift name."""
    match hour:
        case h if 6 <= h < 14:
            return "morning"
        case h if 14 <= h < 22:
            return "afternoon"
        case _:
            return "night"


def _compute_utilization(scheduled_min: float, shift: str) -> float:
    """Return utilization percentage for the given shift."""
    capacity = SHIFT_CAPACITY.get(shift, 480)
    if capacity == 0:
        return 0.0
    return min((scheduled_min / capacity) * 100, 100.0)


def _validate_schedule_row(row: ScheduleRow) -> str:
    """Check a single schedule row for common issues."""
    match (row.get("product_id"), row.get("quantity")):
        case (None, _):
            return "missing_product"
        case (_, None):
            return "missing_quantity"
        case (_, qty) if qty <= 0:
            return "invalid_quantity"
        case _:
            return "valid"


def build_production_schedule(
    df: pd.DataFrame,
    shift: str = "all",
    planning_horizon_days: int = 7,
) -> pd.DataFrame:
    """Generate a production schedule from historical throughput data.

    Uses recent production rates per line to project forward capacity and
    assign scheduling slots.
    """
    prod = df[df["record_type"] == "production"].copy()
    prod["shift"] = prod["timestamp"].dt.hour.apply(_resolve_shift)

    if shift != "all":
        prod = prod[prod["shift"] == shift]

    throughput = (
        prod.groupby(["plant_id", "line_id", "shift"])
        .agg(
            avg_output=("quantity_normalized", "mean"),
            run_count=("timestamp", "count"),
        )
        .reset_index()
    )

    throughput["utilization_pct"] = throughput.apply(
        lambda r: _compute_utilization(r["run_count"] * 5, r["shift"]), axis=1
    )

    # project forward schedule slots
    today = date.today()
    schedule_rows: list[ScheduleRow] = []
    for _, row in throughput.iterrows():
        for day_offset in range(planning_horizon_days):
            schedule_rows.append({
                "plant_id": row["plant_id"],
                "line_id": row["line_id"],
                "shift": row["shift"],
                "scheduled_date": today + timedelta(days=day_offset),
                "projected_output": row["avg_output"],
                "utilization_pct": row["utilization_pct"],
            })

    schedule = pd.DataFrame(schedule_rows)
    logger.info(f"Built schedule: {len(schedule)} slots over {planning_horizon_days} days")
    return schedule
