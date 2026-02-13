"""Pandera schemas for HR data validation."""

import pandera as pa
from pandera import Column, Check, Index
from datetime import date

type EmployeeID = str | int
type SalaryAmount = float | int


employee_schema = pa.DataFrameSchema(
    {
        "employee_id": Column(str, Check.str_matches(r"^EMP\d{5,8}$")),
        "first_name": Column(str, Check.str_length(min_value=1, max_value=100)),
        "last_name": Column(str, Check.str_length(min_value=1, max_value=100)),
        "email": Column(str, Check.str_matches(r"^[\w.+-]+@[\w-]+\.[\w.]+$")),
        "hire_date": Column(pa.DateTime, nullable=False),
        "termination_date": Column(pa.DateTime, nullable=True),
        "department": Column(str, nullable=False),
        "job_title": Column(str, nullable=False),
        "employment_type": Column(
            str,
            Check.isin(["full_time", "part_time", "contractor", "intern", "temp"]),
        ),
        "base_salary": Column(float, Check.greater_than_or_equal_to(0)),
        "currency": Column(str, Check.str_length(3, 3)),
        "manager_id": Column(str, nullable=True),
        "location": Column(str, nullable=False),
        "is_active": Column(bool),
    },
    index=Index(int),
    strict=False,
    coerce=True,
)


headcount_schema = pa.DataFrameSchema(
    {
        "snapshot_date": Column(pa.DateTime),
        "department": Column(str),
        "headcount": Column(int, Check.greater_than_or_equal_to(0)),
        "fte_count": Column(float, Check.greater_than_or_equal_to(0)),
        "contractor_count": Column(int, Check.greater_than_or_equal_to(0)),
        "open_reqs": Column(int, Check.greater_than_or_equal_to(0)),
    },
    coerce=True,
)


compensation_schema = pa.DataFrameSchema(
    {
        "band": Column(str),
        "level": Column(str),
        "min_salary": Column(float, Check.greater_than(0)),
        "median_salary": Column(float, Check.greater_than(0)),
        "max_salary": Column(float, Check.greater_than(0)),
        "employee_count": Column(int, Check.greater_than_or_equal_to(0)),
        "compa_ratio_mean": Column(float, Check.in_range(0.5, 2.0)),
    },
    coerce=True,
)


recruiting_schema = pa.DataFrameSchema(
    {
        "requisition_id": Column(str),
        "stage": Column(
            str,
            Check.isin([
                "applied", "phone_screen", "onsite", "offer", "hired", "rejected",
            ]),
        ),
        "candidate_count": Column(int, Check.greater_than_or_equal_to(0)),
        "avg_days_in_stage": Column(float, nullable=True),
    },
    coerce=True,
)
