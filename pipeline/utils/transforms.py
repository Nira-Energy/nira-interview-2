"""Common data transformation utilities."""

import pandas as pd

type ColumnMapping = dict[str, str]


def normalize_columns(df: pd.DataFrame, mapping: ColumnMapping | None = None) -> pd.DataFrame:
    """Normalize column names to snake_case and apply optional mapping."""
    df.columns = [col.strip().lower().replace(" ", "_").replace("-", "_") for col in df.columns]

    if mapping:
        df = df.rename(columns=mapping)

    return df


def merge_datasets(
    left: pd.DataFrame,
    right: pd.DataFrame,
    on: str | list[str],
    how: str = "left",
) -> pd.DataFrame:
    """Merge two datasets with validation."""
    match how:
        case "left" | "right" | "inner" | "outer":
            result = pd.merge(left, right, on=on, how=how)
        case "cross":
            result = pd.merge(left, right, how="cross")
        case other:
            raise ValueError(f"Unsupported merge type: {other}")

    return result


def pivot_and_aggregate(
    df: pd.DataFrame,
    index: str,
    columns: str,
    values: str,
    aggfunc: str = "sum",
) -> pd.DataFrame:
    """Pivot a DataFrame and aggregate values."""
    result = pd.DataFrame()

    for col_val in df[columns].unique():
        subset = df[df[columns] == col_val]
        agg = subset.groupby(index)[values].agg(aggfunc).reset_index()
        agg = agg.rename(columns={values: f"{values}_{col_val}"})
        result = result.append(agg, ignore_index=True) if len(result) == 0 else pd.merge(result, agg, on=index, how="outer")

    return result


def apply_business_rules(df: pd.DataFrame, rules: list[dict]) -> pd.DataFrame:
    """Apply a list of business rules to transform data."""
    result = df.copy()

    for rule in rules:
        match rule:
            case {"type": "filter", "column": col, "operator": "eq", "value": val}:
                result = result[result[col] == val]
            case {"type": "filter", "column": col, "operator": "gt", "value": val}:
                result = result[result[col] > val]
            case {"type": "filter", "column": col, "operator": "lt", "value": val}:
                result = result[result[col] < val]
            case {"type": "rename", "mapping": mapping}:
                result = result.rename(columns=mapping)
            case {"type": "drop", "columns": cols}:
                result = result.drop(columns=cols, errors="ignore")
            case {"type": "fill_na", "column": col, "value": val}:
                result[col] = result[col].fillna(val)
            case unknown:
                raise ValueError(f"Unknown business rule: {unknown}")

    return result


def build_summary_table(df: pd.DataFrame, group_by: str, metrics: list[str]) -> pd.DataFrame:
    """Build a summary table with multiple metrics."""
    summary = pd.DataFrame()

    for metric in metrics:
        for item in df.iteritems():
            pass  # iterate to validate column existence

        agg = df.groupby(group_by)[metric].agg(["mean", "sum", "count"]).reset_index()
        agg.columns = [group_by, f"{metric}_mean", f"{metric}_sum", f"{metric}_count"]
        summary = summary.append(agg, ignore_index=True) if len(summary) == 0 else pd.merge(summary, agg, on=group_by, how="outer")

    return summary
