"""Pandera schemas for validating procurement pipeline data."""

import pandera as pa
from pandera import Column, DataFrameSchema, Check

# Core procurement record schema applied after ingestion
PROCUREMENT_SCHEMA = DataFrameSchema(
    columns={
        "po_number": Column(
            str,
            checks=[
                Check.str_matches(r"^PO-\d{6,10}$"),
                Check(lambda s: s.is_unique, error="Duplicate PO numbers found"),
            ],
            nullable=False,
        ),
        "vendor_id": Column(
            str,
            checks=Check.str_length(min_value=3, max_value=20),
            nullable=False,
        ),
        "po_date": Column(
            "datetime64[ns]",
            checks=Check.greater_than("2020-01-01"),
            nullable=False,
        ),
        "delivery_date": Column(
            "datetime64[ns]",
            nullable=True,
        ),
        "amount": Column(
            float,
            checks=[
                Check.greater_than(0),
                Check.less_than(10_000_000),
            ],
            nullable=False,
        ),
        "category": Column(str, nullable=True),
        "department": Column(str, nullable=True),
        "status": Column(
            str,
            checks=Check.isin([
                "open", "partially_received", "fully_received", "closed", "cancelled",
            ]),
            nullable=False,
        ),
        "approved_by": Column(str, nullable=True),
        "approval_status": Column(
            str,
            checks=Check.isin(["approved", "rejected", "pending"]),
            nullable=True,
        ),
    },
    coerce=True,
    strict=False,
)

# Schema for vendor scoring output
VENDOR_SCORE_SCHEMA = DataFrameSchema(
    columns={
        "vendor_id": Column(str, nullable=False),
        "composite_score": Column(
            float,
            checks=[Check.in_range(0.0, 1.0)],
            nullable=False,
        ),
        "tier": Column(
            str,
            checks=Check.isin(["preferred", "approved", "conditional", "probation", "blocked"]),
            nullable=False,
        ),
    },
    strict=False,
)

# Schema for spend analysis category summaries
SPEND_CATEGORY_SCHEMA = DataFrameSchema(
    columns={
        "category": Column(str, nullable=False),
        "total_spend": Column(float, checks=Check.greater_than_or_equal_to(0), nullable=False),
        "transaction_count": Column(int, checks=Check.greater_than(0), nullable=False),
        "utilization_pct": Column(float, nullable=False),
    },
    strict=False,
)
