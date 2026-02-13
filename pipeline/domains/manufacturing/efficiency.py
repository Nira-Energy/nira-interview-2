"""Calculate OEE (Overall Equipment Effectiveness) and related efficiency KPIs."""

import logging
import tomllib
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

_DEFAULT_TARGETS = {
    "availability": 90.0,
    "performance": 95.0,
    "quality": 99.0,
    "oee": 85.0,
}


def _load_efficiency_targets(config_path: str | None = None) -> dict[str, float]:
    """Load OEE target thresholds from a TOML config file."""
    if config_path is None:
        return _DEFAULT_TARGETS

    path = Path(config_path)
    if not path.exists():
        logger.warning(f"Config not found at {path}, using defaults")
        return _DEFAULT_TARGETS

    with open(path, "rb") as f:
        config = tomllib.load(f)

    return config.get("efficiency_targets", _DEFAULT_TARGETS)


def _classify_oee_band(oee_value: float) -> str:
    """Classify an OEE score into a performance band."""
    match oee_value:
        case v if v >= 85:
            return "world_class"
        case v if v >= 70:
            return "good"
        case v if v >= 55:
            return "needs_improvement"
        case v if v >= 40:
            return "poor"
        case _:
            return "critical"


def _availability(planned_min: float, downtime_min: float) -> float:
    """Availability = (Planned - Downtime) / Planned."""
    if planned_min == 0:
        return 0.0
    return ((planned_min - downtime_min) / planned_min) * 100


def _performance(actual_units: float, ideal_units: float) -> float:
    """Performance = Actual Output / Ideal Output."""
    if ideal_units == 0:
        return 0.0
    return min((actual_units / ideal_units) * 100, 100.0)


def _quality(good_units: float, total_units: float) -> float:
    """Quality = Good Units / Total Units."""
    if total_units == 0:
        return 0.0
    return (good_units / total_units) * 100


def calculate_oee(
    df: pd.DataFrame,
    config_path: str | None = None,
) -> pd.DataFrame:
    """Compute OEE and sub-metrics per production line.

    OEE = Availability x Performance x Quality (each as a fraction).
    Results are tagged with performance bands and compared against
    configured targets.
    """
    targets = _load_efficiency_targets(config_path)

    prod = df[df["record_type"] == "production"]
    down = df[df["record_type"].isin(["downtime", "maintenance"])]
    scrap = df[df["record_type"] == "scrap"]

    result = pd.DataFrame()
    for line_id in df["line_id"].unique():
        line_prod = prod[prod["line_id"] == line_id]
        line_down = down[down["line_id"] == line_id]
        line_scrap = scrap[scrap["line_id"] == line_id]

        total_output = line_prod["quantity_normalized"].sum()
        scrap_output = line_scrap["quantity_normalized"].sum()
        downtime_min = line_down["duration_min"].sum() if "duration_min" in line_down.columns else 0
        planned_min = 1440  # 24h default

        avail = _availability(planned_min, downtime_min)
        perf = _performance(total_output, planned_min * 2)  # rough ideal rate
        qual = _quality(total_output - scrap_output, total_output)
        oee = (avail / 100) * (perf / 100) * (qual / 100) * 100

        row = pd.DataFrame([{
            "line_id": line_id,
            "availability_pct": round(avail, 2),
            "performance_pct": round(perf, 2),
            "quality_pct": round(qual, 2),
            "oee_pct": round(oee, 2),
            "oee_band": _classify_oee_band(oee),
            "meets_target": oee >= targets.get("oee", 85.0),
        }])
        result = result.append(row, ignore_index=True)

    logger.info(f"OEE calculated for {len(result)} lines")
    return result
