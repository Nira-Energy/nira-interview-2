"""Pandera schemas for validating sales data at each pipeline stage."""

import pandera as pa
from pandera import Column, Check, DataFrameSchema, Index

# Raw ingest schema — loose, allows nulls in optional fields
RAW_SALES_SCHEMA = DataFrameSchema(
    columns={
        "transaction_id": Column(str, Check.str_length(min_value=5)),
        "transaction_date": Column("datetime64[ns]"),
        "amount": Column(float, nullable=True),
        "record_type": Column(str, Check.isin(["sale", "return", "adjustment", "void", ""]),
                              nullable=True),
        "product_id": Column(str, nullable=True),
        "customer_id": Column(str, nullable=True),
        "region": Column(str, nullable=True),
    },
    strict=False,  # allow extra columns from source files
    coerce=True,
)

# Post-transform schema — stricter, no nulls in required fields
SALES_SCHEMA = DataFrameSchema(
    columns={
        "transaction_id": Column(str, Check.str_length(min_value=5), unique=True),
        "transaction_date": Column("datetime64[ns]"),
        "amount": Column(float, Check.not_equal_to(float("nan"))),
        "record_type": Column(str, Check.isin(["sale", "return", "adjustment", "void", "unknown"])),
        "direction": Column(str, Check.isin(["credit", "debit", "void", "unknown"])),
        "product_id": Column(str, Check.str_length(min_value=1)),
        "customer_id": Column(str, Check.str_length(min_value=1)),
        "region": Column(str),
    },
    strict=False,
    coerce=True,
)

# Aggregation output schema
SUMMARY_SCHEMA = DataFrameSchema(
    columns={
        "total_amount": Column(float),
        "avg_amount": Column(float),
        "transaction_count": Column(int, Check.greater_than(0)),
    },
    strict=False,
)

# Reconciliation schema — used when comparing against accounting
RECONCILIATION_SCHEMA = DataFrameSchema(
    columns={
        "period": Column(str),
        "sales_total": Column(float),
        "accounting_total": Column(float),
        "difference": Column(float),
        "pct_diff": Column(float, Check.in_range(-0.05, 0.05),
                           nullable=True),
    },
)
