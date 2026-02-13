"""Pandera schemas for financial data validation."""

import pandera as pa
from pandera import Column, Check, Index

type SchemaMapping = dict[str, pa.DataFrameSchema]


GL_LINE_SCHEMA = pa.DataFrameSchema(
    {
        "journal_id": Column(str, Check.str_matches(r"^JE-\d{8}-\d{4}$")),
        "posting_date": Column("datetime64[ns]"),
        "effective_date": Column("datetime64[ns]", nullable=True),
        "account_code": Column(str, Check.str_matches(r"^\d{4,6}$")),
        "account_name": Column(str, Check.str_length(min_value=1, max_value=200)),
        "debit": Column(float, Check.ge(0), nullable=True),
        "credit": Column(float, Check.ge(0), nullable=True),
        "description": Column(str, nullable=True),
        "entity_code": Column(str, Check.str_matches(r"^ENT-\d{3}$")),
        "cost_center": Column(str, nullable=True),
        "fiscal_period": Column(str, Check.str_matches(r"^\d{4}-\d{2}$")),
    },
    checks=[
        Check(
            lambda df: (df["debit"].fillna(0) + df["credit"].fillna(0)) > 0,
            error="Each line must have a debit or credit amount",
        ),
    ],
    coerce=True,
)


AP_AR_SCHEMA = pa.DataFrameSchema(
    {
        "invoice_id": Column(str, unique=True),
        "vendor_or_customer": Column(str),
        "invoice_date": Column("datetime64[ns]"),
        "due_date": Column("datetime64[ns]"),
        "amount": Column(float, Check.gt(0)),
        "currency": Column(str, Check.str_length(3, 3)),
        "status": Column(str, Check.isin(["open", "paid", "partial", "void", "disputed"])),
        "subledger": Column(str, Check.isin(["AP", "AR"])),
    },
    coerce=True,
)


BUDGET_SCHEMA = pa.DataFrameSchema(
    {
        "account_code": Column(str),
        "fiscal_period": Column(str),
        "budget_amount": Column(float),
        "department": Column(str, nullable=True),
    },
    coerce=True,
)


FINANCE_SCHEMA = GL_LINE_SCHEMA


SCHEMA_REGISTRY: SchemaMapping = {
    "gl_line": GL_LINE_SCHEMA,
    "ap_ar": AP_AR_SCHEMA,
    "budget": BUDGET_SCHEMA,
}
