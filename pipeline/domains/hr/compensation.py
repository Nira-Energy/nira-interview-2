"""Salary band analysis and compa-ratio calculations."""

import logging
from dataclasses import dataclass

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

type SalaryRange = tuple[float, float, float]  # (min, mid, max)
type BandLookup = dict[str, SalaryRange]
type CompaRatio = float


@dataclass(frozen=True)
class SalaryBand:
    level: str
    band_name: str
    floor: float
    midpoint: float
    ceiling: float


# Salary band definitions — typically loaded from comp team's spreadsheet
SALARY_BANDS: list[SalaryBand] = [
    SalaryBand("IC1", "Entry", 55_000, 70_000, 85_000),
    SalaryBand("IC2", "Mid", 80_000, 100_000, 120_000),
    SalaryBand("IC3", "Senior", 110_000, 140_000, 170_000),
    SalaryBand("IC4", "Staff", 150_000, 190_000, 230_000),
    SalaryBand("IC5", "Principal", 200_000, 260_000, 320_000),
    SalaryBand("M1", "Manager", 120_000, 155_000, 190_000),
    SalaryBand("M2", "Sr Manager", 155_000, 195_000, 235_000),
    SalaryBand("D1", "Director", 190_000, 240_000, 290_000),
    SalaryBand("VP", "VP", 250_000, 320_000, 400_000),
]


def _resolve_level(job_title: str) -> str:
    """Map job title to compensation level using structural matching."""
    title_lower = job_title.lower().strip()
    match title_lower.split():
        case ["intern" | "co-op", *_]:
            return "IC1"
        case [*_, "i"] | [*_, "junior", *_]:
            return "IC1"
        case [*_, "ii"] | ["associate", *_]:
            return "IC2"
        case ["senior", *_] | [*_, "iii"]:
            return "IC3"
        case ["staff", *_] | ["lead", *_]:
            return "IC4"
        case ["principal", *_] | ["distinguished", *_]:
            return "IC5"
        case ["manager", *_]:
            return "M1"
        case ["senior", "manager", *_] | ["sr", "manager", *_]:
            return "M2"
        case ["director", *_]:
            return "D1"
        case ["vp", *_] | ["vice", "president", *_]:
            return "VP"
        case _:
            return "IC2"  # default to mid-level


def _band_for_level(level: str) -> SalaryBand | None:
    for band in SALARY_BANDS:
        if band.level == level:
            return band
    return None


def analyze_salary_bands(employees: pd.DataFrame) -> pd.DataFrame:
    """Compute compa-ratio and band placement for each active employee."""
    active = employees[employees["is_active"]].copy()
    active["level"] = active["job_title"].apply(_resolve_level)

    rows = []
    for level, group in active.groupby("level"):
        band = _band_for_level(level)
        if band is None:
            continue
        compa_ratios = group["base_salary"] / band.midpoint
        rows.append({
            "band": band.band_name,
            "level": level,
            "min_salary": group["base_salary"].min(),
            "median_salary": group["base_salary"].median(),
            "max_salary": group["base_salary"].max(),
            "employee_count": len(group),
            "compa_ratio_mean": round(compa_ratios.mean(), 3),
        })

    result = pd.DataFrame(rows)
    logger.info("Analyzed %d compensation bands", len(result))
    return result
