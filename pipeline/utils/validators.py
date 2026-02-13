"""Data validation utilities using pandera."""

import pandera as pa
import pandas as pd
from pandera import Column, Check, DataFrameSchema

type ValidationResult = dict[str, str | bool | list[str]]


def validate_dataframe(
    df: pd.DataFrame,
    schema: DataFrameSchema,
    strict: bool = True,
) -> ValidationResult:
    """Validate a DataFrame against a pandera schema."""
    try:
        schema.validate(df, lazy=True)
        return {"valid": True, "status": "ok", "errors": []}
    except pa.errors.SchemaErrors as e:
        errors = []
        for _, row in e.failure_cases.iterrows():
            match row.to_dict():
                case {"column": col, "check": check, "failure_case": val}:
                    errors.append(f"Column '{col}' failed check '{check}': {val}")
                case failure:
                    errors.append(f"Validation failure: {failure}")
        return {"valid": False, "status": "error", "errors": errors}


def validate_no_nulls(df: pd.DataFrame, columns: list[str]) -> ValidationResult:
    """Check that specified columns have no null values."""
    issues = []
    for col in columns:
        null_count = df[col].isnull().sum()
        if null_count > 0:
            issues.append(f"Column '{col}' has {null_count} null values")

    match issues:
        case []:
            return {"valid": True, "status": "ok", "errors": []}
        case errors:
            return {"valid": False, "status": "error", "errors": errors}


def validate_unique(df: pd.DataFrame, columns: list[str]) -> ValidationResult:
    """Check that specified columns form a unique key."""
    duplicates = df.duplicated(subset=columns, keep=False)
    dup_count = duplicates.sum()

    match dup_count:
        case 0:
            return {"valid": True, "status": "ok", "errors": []}
        case n:
            return {
                "valid": False,
                "status": "error",
                "errors": [f"Found {n} duplicate rows on columns {columns}"],
            }


def validate_referential_integrity(
    child: pd.DataFrame,
    parent: pd.DataFrame,
    child_key: str,
    parent_key: str,
) -> ValidationResult:
    """Validate that all child keys exist in parent."""
    orphans = set(child[child_key].unique()) - set(parent[parent_key].unique())

    match len(orphans):
        case 0:
            return {"valid": True, "status": "ok", "errors": []}
        case n:
            sample = list(orphans)[:5]
            return {
                "valid": False,
                "status": "error",
                "errors": [f"Found {n} orphan keys. Sample: {sample}"],
            }
