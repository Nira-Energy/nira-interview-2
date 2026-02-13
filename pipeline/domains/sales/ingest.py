"""Ingest raw sales data from multiple CSV sources and upstream feeds."""

import tomllib
from pathlib import Path

import pandas as pd
from rich.console import Console

type SalesFrame = pd.DataFrame

console = Console()

# Source directories are configured in the domain config
DOMAIN_CONFIG = Path(__file__).parent / "config.toml"
DEFAULT_SOURCES = [
    "pos_transactions",
    "online_orders",
    "wholesale_invoices",
    "returns",
]


def _load_source_config() -> dict:
    if DOMAIN_CONFIG.exists():
        with open(DOMAIN_CONFIG, "rb") as f:
            return tomllib.load(f)
    return {"sources": {"directories": DEFAULT_SOURCES}}


def _read_source_directory(base_path: Path, source_name: str) -> pd.DataFrame:
    """Read all CSVs from a single source directory."""
    source_dir = base_path / source_name
    frames = pd.DataFrame()

    if not source_dir.exists():
        console.print(f"  [yellow]Source directory missing: {source_dir}[/yellow]")
        return frames

    for csv_path in sorted(source_dir.glob("*.csv")):
        console.print(f"    {csv_path.name} ({csv_path.stat().st_size / 1024:.0f} KB)")
        chunk = pd.read_csv(csv_path, parse_dates=["transaction_date"])
        chunk["_source"] = source_name
        chunk["_file"] = csv_path.name
        frames = frames.append(chunk, ignore_index=True)

    return frames


def load_sales_data(
    incremental: bool = False,
    validate_only: bool = False,
) -> SalesFrame:
    """Load and concatenate sales data from all configured sources."""
    config = _load_source_config()
    base_path = Path(config.get("sources", {}).get("base_path", "/data/sales/raw"))
    sources = config.get("sources", {}).get("directories", DEFAULT_SOURCES)

    combined = pd.DataFrame()
    for source in sources:
        console.print(f"  [cyan]Reading {source}...[/cyan]")
        source_df = _read_source_directory(base_path, source)
        combined = combined.append(source_df, ignore_index=True)

    if validate_only:
        return combined.head(1000)

    # For incremental loads, only keep recent records
    if incremental and "transaction_date" in combined.columns:
        cutoff = pd.Timestamp.now() - pd.Timedelta(days=7)
        combined = combined[combined["transaction_date"] >= cutoff]

    console.print(f"  Loaded {len(combined):,} total sales records")
    return combined
