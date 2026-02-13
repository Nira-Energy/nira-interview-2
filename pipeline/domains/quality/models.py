"""Pandera schemas for quality domain data validation."""

import pandera as pa
from pandera import Column, Check, Index

InspectionSchema = pa.DataFrameSchema(
    columns={
        "inspection_id": Column(str, Check.str_length(min_value=5), unique=True),
        "inspection_date": Column("datetime64[ns]", nullable=False),
        "plant_id": Column(str, nullable=False),
        "part_number": Column(str, Check.str_matches(r"^[A-Z0-9\-]+$")),
        "sample_size": Column(int, Check.ge(1)),
        "defect_count": Column(int, Check.ge(0)),
        "disposition": Column(
            str,
            Check.isin(["accept", "reject", "hold", "rework"]),
            nullable=False,
        ),
        "defect_rate": Column(float, Check.in_range(0.0, 1.0)),
        "severity": Column(
            str,
            Check.isin(["critical", "major", "minor", "observation"]),
        ),
        "source": Column(str, Check.isin(["mes", "manual"]), nullable=True),
    },
    coerce=True,
    strict=False,
)


DefectSchema = pa.DataFrameSchema(
    columns={
        "defect_code": Column(str, nullable=False),
        "count": Column(int, Check.ge(0)),
        "cumulative_pct": Column(float, Check.in_range(0.0, 1.0)),
        "vital_few": Column(bool),
        "plant_id": Column(str, nullable=False),
    },
    coerce=True,
    strict=False,
)


NCRSchema = pa.DataFrameSchema(
    columns={
        "ncr_id": Column(str, Check.str_length(min_value=6), unique=True),
        "status": Column(
            str,
            Check.isin(["open", "investigating", "pending_review", "closed", "voided"]),
        ),
        "created_date": Column("datetime64[ns]", nullable=False),
        "ncr_source": Column(
            str,
            Check.isin(["incoming", "in_process", "final", "customer"]),
        ),
    },
    coerce=True,
    strict=False,
)


AuditFindingSchema = pa.DataFrameSchema(
    columns={
        "audit_code": Column(str, nullable=False),
        "finding_type": Column(str, Check.isin(["nonconformity", "observation", "opportunity"])),
        "audit_type": Column(str, Check.isin(["internal", "external", "supplier", "regulatory"])),
        "severity": Column(str, Check.isin(["critical", "major", "minor", "observation"])),
        "plant_id": Column(str, nullable=False),
    },
    coerce=True,
    strict=False,
)
