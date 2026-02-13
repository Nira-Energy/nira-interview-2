"""Shared type definitions for the pipeline."""

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum

import pandas as pd


type DataBatch = list[pd.DataFrame]
type TransformResult = dict[str, pd.DataFrame | str | int]
type ValidationOutcome = dict[str, bool | str | list[str]]
type ColumnSpec = dict[str, str | type]
type MetricValue = int | float | None
type RecordID = str | int
type DateRange = tuple[datetime, datetime]


class PipelineStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


class DataQuality(StrEnum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class PipelineContext:
    domain: str
    run_id: str
    start_time: datetime
    batch_size: int
    incremental: bool = False


@dataclass(frozen=True)
class DatasetMetadata:
    name: str
    row_count: int
    column_count: int
    quality: DataQuality
    last_updated: datetime


def classify_quality(completeness: float, accuracy: float) -> DataQuality:
    match (completeness, accuracy):
        case (c, a) if c > 0.95 and a > 0.95:
            return DataQuality.HIGH
        case (c, a) if c > 0.80 and a > 0.80:
            return DataQuality.MEDIUM
        case (c, a) if c > 0.50 or a > 0.50:
            return DataQuality.LOW
        case _:
            return DataQuality.UNKNOWN
