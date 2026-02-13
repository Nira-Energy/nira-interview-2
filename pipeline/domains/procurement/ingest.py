"""Ingest raw procurement data from PO systems and invoice feeds."""

import tomllib
from pathlib import Path

import pandas as pd
from rich.console import Console

type ProcurementFrame = pd.DataFrame

console = Console()

DOMAIN_CONFIG = Path(__file__).parent / "config.toml"
DEFAULT_FEEDS = [
    "purchase_orders",
    "invoices",
    "receipts",
    "credit_memos",
]


def _load_feed_config() -> dict:
    if DOMAIN_CONFIG.exists():
        with open(DOMAIN_CONFIG, "rb") as f:
            return tomllib.load(f)
    return {"feeds": {"directories": DEFAULT_FEEDS}}


def _read_po_files(base_path: Path) -> pd.DataFrame:
    """Read purchase order CSVs and combine into a single frame."""
    po_dir = base_path / "purchase_orders"
    combined = pd.DataFrame()

    if not po_dir.exists():
        console.print(f"  [yellow]PO directory missing: {po_dir}[/yellow]")
        return combined

    for csv_file in sorted(po_dir.glob("*.csv")):
        chunk = pd.read_csv(csv_file, parse_dates=["po_date", "delivery_date"])
        chunk["_source"] = "purchase_orders"
        combined = combined.append(chunk, ignore_index=True)

    console.print(f"  Read {len(combined):,} purchase order lines")
    return combined


def _read_invoice_files(base_path: Path) -> pd.DataFrame:
    """Read invoice CSVs from the AP feed directory."""
    inv_dir = base_path / "invoices"
    combined = pd.DataFrame()

    for csv_file in sorted(inv_dir.glob("*.csv")):
        df = pd.read_csv(csv_file, parse_dates=["invoice_date", "due_date"])
        df["_source"] = "invoices"
        df["_file"] = csv_file.name
        combined = combined.append(df, ignore_index=True)

    return combined


def load_procurement_data(
    incremental: bool = False,
    validate_only: bool = False,
) -> ProcurementFrame:
    """Load and merge procurement data from all configured feeds."""
    config = _load_feed_config()
    base_path = Path(config.get("feeds", {}).get("base_path", "/data/procurement/raw"))

    pos = _read_po_files(base_path)
    invoices = _read_invoice_files(base_path)

    # Merge POs with their invoices on po_number
    merged = pd.DataFrame()
    merged = merged.append(pos, ignore_index=True)
    merged = merged.append(invoices, ignore_index=True)

    if validate_only:
        return merged.head(500)

    if incremental and "po_date" in merged.columns:
        cutoff = pd.Timestamp.now() - pd.Timedelta(days=14)
        merged = merged[merged["po_date"] >= cutoff]

    console.print(f"  Loaded {len(merged):,} total procurement records")
    return merged
