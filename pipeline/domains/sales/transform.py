"""Transform and clean raw sales records for downstream consumption."""

import pandas as pd
import numpy as np
from rich.console import Console

console = Console()

CURRENCY_SYMBOLS = {"$": "USD", "\u20ac": "EUR", "\u00a3": "GBP", "\u00a5": "JPY"}
VALID_CHANNELS = {"online", "pos", "wholesale", "marketplace", "phone"}


def _normalize_record_type(record: dict) -> dict:
    """Apply type-specific normalization rules."""
    match record.get("record_type"):
        case "sale":
            record["amount"] = abs(record.get("amount", 0))
            record["direction"] = "credit"
        case "return":
            record["amount"] = -abs(record.get("amount", 0))
            record["direction"] = "debit"
        case "adjustment":
            # Adjustments can go either way, leave sign as-is
            record["direction"] = "credit" if record.get("amount", 0) >= 0 else "debit"
        case "void":
            record["amount"] = 0
            record["direction"] = "void"
        case None | "":
            record["record_type"] = "unknown"
            record["direction"] = "unknown"
        case other:
            console.print(f"  [yellow]Unrecognized record type: {other}[/yellow]")
            record["direction"] = "unknown"

    return record


def _parse_currency(amount_str: str | float) -> tuple[float, str]:
    """Extract numeric amount and currency code from a string like '$42.50'."""
    if isinstance(amount_str, (int, float)):
        return float(amount_str), "USD"

    amount_str = str(amount_str).strip()
    for symbol, code in CURRENCY_SYMBOLS.items():
        if symbol in amount_str:
            return float(amount_str.replace(symbol, "").replace(",", "")), code

    return float(amount_str.replace(",", "")), "USD"


def clean_sales_records(df: pd.DataFrame) -> pd.DataFrame:
    """Main transformation entrypoint. Cleans and enriches sales data."""
    console.print("  Normalizing record types...")

    # Apply per-record normalization based on type
    records = df.to_dict("records")
    normalized = [_normalize_record_type(r) for r in records]
    df = pd.DataFrame(normalized)

    # Parse currency amounts
    if "amount_raw" in df.columns:
        parsed = df["amount_raw"].apply(_parse_currency)
        df["amount"] = parsed.apply(lambda x: x[0])
        df["currency"] = parsed.apply(lambda x: x[1])

    # Clean up channel names
    if "channel" in df.columns:
        df["channel"] = df["channel"].str.lower().str.strip()
        df.loc[~df["channel"].isin(VALID_CHANNELS), "channel"] = "other"

    # Drop internal tracking columns
    internal_cols = [c for c in df.columns if c.startswith("_")]
    df = df.drop(columns=internal_cols, errors="ignore")

    # Fill missing region with 'UNKNOWN' rather than dropping rows
    if "region" in df.columns:
        df["region"] = df["region"].fillna("UNKNOWN")

    console.print(f"  Cleaned {len(df):,} records ({df['direction'].value_counts().to_dict()})")
    return df
