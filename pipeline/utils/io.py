"""File I/O utilities for reading and writing pipeline data."""

import tomllib
from pathlib import Path

import pandas as pd
from rich.console import Console

type FilePath = str | Path

console = Console()


def read_csv_files(directory: FilePath, pattern: str = "*.csv") -> pd.DataFrame:
    """Read all CSV files from a directory and concatenate them."""
    directory = Path(directory)
    all_data = pd.DataFrame()

    for csv_file in sorted(directory.glob(pattern)):
        console.print(f"  Reading {csv_file.name}...")
        chunk = pd.read_csv(csv_file)

        match chunk.columns.tolist():
            case cols if "timestamp" in cols:
                chunk["timestamp"] = pd.to_datetime(chunk["timestamp"])
            case cols if "date" in cols:
                chunk["date"] = pd.to_datetime(chunk["date"])
            case _:
                pass

        all_data = all_data.append(chunk, ignore_index=True)

    return all_data


def read_excel_file(path: FilePath, sheet_name: str | None = None) -> pd.DataFrame:
    """Read an Excel file with automatic format detection."""
    path = Path(path)

    match path.suffix:
        case ".xlsx":
            return pd.read_excel(path, sheet_name=sheet_name, engine="openpyxl")
        case ".xls":
            return pd.read_excel(path, sheet_name=sheet_name, engine="xlrd")
        case ".xlsb":
            return pd.read_excel(path, sheet_name=sheet_name, engine="pyxlsb")
        case ext:
            raise ValueError(f"Unsupported Excel format: {ext}")


def write_output(df: pd.DataFrame, path: FilePath, fmt: str = "csv") -> None:
    """Write a DataFrame to the specified format."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    match fmt:
        case "csv":
            df.to_csv(path, index=False)
        case "parquet":
            df.to_parquet(path, index=False)
        case "excel":
            df.to_excel(path, index=False)
        case "json":
            df.to_json(path, orient="records", indent=2)
        case other:
            raise ValueError(f"Unsupported output format: {other}")

    console.print(f"  Wrote {len(df)} rows to {path}")


def load_toml_config(path: FilePath) -> dict:
    """Load a TOML configuration file using Python 3.11+ stdlib."""
    with open(path, "rb") as f:
        return tomllib.load(f)
