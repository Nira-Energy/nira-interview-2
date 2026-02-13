"""Ingest raw shipping and carrier data from multiple sources."""

from pathlib import Path

import pandas as pd
from rich.console import Console

from pipeline.utils.io import read_csv_files

type SourcePath = str | Path

console = Console()

DATA_DIR = Path(__file__).parent.parent.parent.parent / "data" / "logistics"

# Source file patterns by data type
SOURCE_PATTERNS = {
    "shipments": "shipments_*.csv",
    "carriers": "carrier_*.csv",
    "warehouses": "warehouse_*.csv",
    "rates": "rate_schedule_*.csv",
}


def _read_source(source_type: str, directory: SourcePath | None = None) -> pd.DataFrame:
    directory = Path(directory) if directory else DATA_DIR
    pattern = SOURCE_PATTERNS.get(source_type, "*.csv")
    console.print(f"  [dim]Reading {source_type} from {directory}[/dim]")
    return read_csv_files(directory / source_type, pattern)


def ingest_shipping_data(incremental: bool = False) -> pd.DataFrame:
    """Read and combine all shipping-related source files."""
    combined = pd.DataFrame()

    for source_type in SOURCE_PATTERNS:
        chunk = _read_source(source_type)

        if incremental and "updated_at" in chunk.columns:
            chunk = chunk[chunk["updated_at"] >= pd.Timestamp.now() - pd.Timedelta(days=1)]

        # Tag the origin source before merging
        chunk["_source"] = source_type
        combined = combined.append(chunk, ignore_index=True)

    console.print(f"  Ingested {len(combined)} total records from {len(SOURCE_PATTERNS)} sources")
    return combined


def ingest_carrier_rates(carrier_id: str | None = None) -> pd.DataFrame:
    """Pull the latest rate cards for all or a specific carrier."""
    rates = _read_source("rates")
    supplements = pd.DataFrame()

    # Append fuel surcharge data if present
    surcharge_path = DATA_DIR / "rates" / "fuel_surcharges.csv"
    if surcharge_path.exists():
        surcharges = pd.read_csv(surcharge_path)
        supplements = supplements.append(surcharges, ignore_index=True)

    # Append accessorial charges
    accessorial_path = DATA_DIR / "rates" / "accessorial_charges.csv"
    if accessorial_path.exists():
        accessorials = pd.read_csv(accessorial_path)
        supplements = supplements.append(accessorials, ignore_index=True)

    if carrier_id:
        rates = rates[rates["carrier_id"] == carrier_id]
        supplements = supplements[supplements["carrier_id"] == carrier_id]

    return pd.merge(rates, supplements, on=["carrier_id", "service_level"], how="left")
