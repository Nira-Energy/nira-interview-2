"""Pandera schemas for manufacturing domain validation."""

import pandera as pa
from pandera import Column, Check, Index

ProductionSchema = pa.DataFrameSchema(
    columns={
        "plant_id": Column(str, Check.str_matches(r"^plant-\d{2}$")),
        "line_id": Column(str, nullable=False),
        "timestamp": Column("datetime64[ns, UTC]", nullable=False),
        "record_type": Column(
            str,
            Check.isin([
                "production",
                "scrap",
                "downtime",
                "maintenance",
                "quality_check",
                "unknown",
            ]),
        ),
        "record_code": Column(str, nullable=True),
        "quantity": Column(float, Check.greater_than_or_equal_to(0)),
        "quantity_normalized": Column(float, Check.greater_than_or_equal_to(0)),
        "product_id": Column(str, nullable=True),
        "ingested_at": Column("datetime64[ns]", nullable=False),
    },
    coerce=True,
    strict=False,
)

DowntimeSchema = pa.DataFrameSchema(
    columns={
        "line_id": Column(str, nullable=False),
        "category": Column(
            str,
            Check.isin([
                "mechanical",
                "electrical",
                "process",
                "external",
                "other",
                "unclassified",
            ]),
        ),
        "severity": Column(
            str,
            Check.isin(["micro_stop", "minor", "moderate", "major", "critical"]),
        ),
        "event_count": Column(int, Check.greater_than(0)),
        "total_minutes": Column(float, Check.greater_than_or_equal_to(0)),
        "avg_duration": Column(float, Check.greater_than_or_equal_to(0)),
        "mtbf_hours": Column(float, Check.greater_than_or_equal_to(0)),
    },
    coerce=True,
    strict=False,
)
