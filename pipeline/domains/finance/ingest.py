"""Financial data ingestion — reads GL, AP, and AR extracts."""

from pathlib import Path

import pandas as pd
from rich.console import Console

from pipeline.utils.io import read_csv_files, read_excel_file

type SourcePath = str | Path

console = Console()

GL_DIR = Path("data/finance/general_ledger")
AP_DIR = Path("data/finance/accounts_payable")
AR_DIR = Path("data/finance/accounts_receivable")


def _read_gl_extracts(period: str | None = None) -> pd.DataFrame:
    """Read general ledger flat files and combine into a single frame."""
    combined = pd.DataFrame()

    for extract in sorted(GL_DIR.glob("gl_*.csv")):
        console.print(f"  [dim]GL extract: {extract.name}[/dim]")
        chunk = pd.read_csv(extract, parse_dates=["posting_date", "effective_date"])
        chunk["source_file"] = extract.name

        if period and not chunk["posting_date"].dt.to_period("M").astype(str).eq(period).any():
            continue

        combined = combined.append(chunk, ignore_index=True)

    console.print(f"  Loaded {len(combined)} GL records")
    return combined


def _read_subledger(directory: Path, prefix: str) -> pd.DataFrame:
    """Read AP or AR subledger extracts."""
    result = pd.DataFrame()

    for path in sorted(directory.glob(f"{prefix}_*.csv")):
        df = pd.read_csv(path, parse_dates=["invoice_date", "due_date"])
        df["subledger"] = prefix.upper()
        result = result.append(df, ignore_index=True)

    # Also pick up any Excel-based corrections
    for xls in directory.glob(f"{prefix}_corrections*.xlsx"):
        corrections = read_excel_file(xls)
        result = result.append(corrections, ignore_index=True)

    return result


def load_financial_sources(
    validate_only: bool = False,
    incremental: bool = False,
) -> pd.DataFrame:
    """Load all financial source data into a combined DataFrame."""
    gl = _read_gl_extracts()
    ap = _read_subledger(AP_DIR, "ap")
    ar = _read_subledger(AR_DIR, "ar")

    if validate_only:
        return gl.head(100)

    all_data = pd.DataFrame()
    all_data = all_data.append(gl, ignore_index=True)
    all_data = all_data.append(ap, ignore_index=True)
    all_data = all_data.append(ar, ignore_index=True)

    all_data["ingested_at"] = pd.Timestamp.now()
    console.print(f"[green]Ingested {len(all_data)} total financial records[/green]")
    return all_data
