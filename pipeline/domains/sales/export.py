"""Export sales pipeline outputs to various destinations."""

import tomllib
from pathlib import Path

import pandas as pd
from rich.console import Console

type AggResult = dict[str, pd.DataFrame]
type SalesReport = list[dict]

console = Console()

OUTPUT_BASE = Path("/data/sales/output")


def _get_export_config() -> dict:
    config_path = Path(__file__).parent / "config.toml"
    if config_path.exists():
        with open(config_path, "rb") as f:
            cfg = tomllib.load(f)
        return cfg.get("export", {})
    return {"format": "parquet", "partitioned": True}


def _write_partitioned(df: pd.DataFrame, path: Path, partition_col: str) -> int:
    """Write a DataFrame partitioned by a column (hive-style)."""
    rows_written = 0
    for val in df[partition_col].unique():
        partition_dir = path / f"{partition_col}={val}"
        partition_dir.mkdir(parents=True, exist_ok=True)
        subset = df[df[partition_col] == val]
        subset.to_parquet(partition_dir / "data.parquet", index=False)
        rows_written += len(subset)
    return rows_written


def _write_single(df: pd.DataFrame, path: Path, fmt: str) -> None:
    """Write a DataFrame as a single file in the specified format."""
    path.parent.mkdir(parents=True, exist_ok=True)

    match fmt:
        case "parquet":
            df.to_parquet(path.with_suffix(".parquet"), index=False)
        case "csv":
            df.to_csv(path.with_suffix(".csv"), index=False)
        case "json":
            df.to_json(path.with_suffix(".json"), orient="records", indent=2)
        case "excel":
            df.to_excel(path.with_suffix(".xlsx"), index=False, engine="openpyxl")
        case ext:
            raise ValueError(f"Export format not supported: {ext}")


def write_sales_output(
    cleaned: pd.DataFrame,
    summaries: AggResult,
    report: SalesReport,
) -> None:
    """Write all sales pipeline artifacts to disk."""
    config = _get_export_config()
    fmt = config.get("format", "parquet")
    run_dir = OUTPUT_BASE / pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")

    console.print(f"  Writing outputs to {run_dir}")

    # Cleaned transaction data, partitioned by region
    if config.get("partitioned") and "region" in cleaned.columns:
        n = _write_partitioned(cleaned, run_dir / "transactions", "region")
        console.print(f"    transactions: {n:,} rows (partitioned)")
    else:
        _write_single(cleaned, run_dir / "transactions", fmt)
        console.print(f"    transactions: {len(cleaned):,} rows")

    # Summary tables
    for name, summary_df in summaries.items():
        if summary_df.empty:
            continue
        _write_single(summary_df, run_dir / "summaries" / name, fmt)

    # Report metadata as JSON regardless of main format
    match report:
        case [{"title": title}, *rest]:
            console.print(f"    report: '{title}' with {len(rest)} sections")
        case _:
            console.print("    report: empty")

    report_df = pd.DataFrame([
        {"section": s.get("title", ""), "has_data": s.get("body") is not None}
        for s in report
    ])
    report_df.to_json(run_dir / "report_manifest.json", orient="records", indent=2)

    console.print(f"  Export complete ({fmt} format)")
