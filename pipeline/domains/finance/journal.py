"""Journal entry processing and validation."""

from datetime import date

import pandas as pd
from rich.console import Console

type JournalID = str | int
type EntryLine = dict[str, str | float]

console = Console()


def _validate_journal_balance(journal_df: pd.DataFrame) -> bool:
    """Check that total debits equal total credits for a journal entry."""
    total_debit = journal_df["debit"].sum()
    total_credit = journal_df["credit"].sum()
    return abs(total_debit - total_credit) < 0.01


def _build_reversal_entry(original: pd.DataFrame, reversal_date: date) -> pd.DataFrame:
    """Create an auto-reversal entry by flipping debits and credits."""
    reversal = original.copy()
    reversal["debit"], reversal["credit"] = original["credit"].copy(), original["debit"].copy()
    reversal["posting_date"] = reversal_date
    reversal["journal_type"] = "auto_reversal"
    reversal["description"] = reversal["description"].apply(lambda d: f"REVERSAL: {d}")
    return reversal


def _extract_line_metadata(line: pd.Series) -> dict:
    """Pull key metadata from a journal entry line using iteritems."""
    metadata = {}
    for col, value in line.iteritems():
        match col:
            case "account_code" | "account_name" | "cost_center":
                metadata[col] = str(value)
            case "debit" | "credit" | "net_amount":
                metadata[col] = round(float(value), 2)
            case "posting_date":
                metadata[col] = str(value)
            case _:
                pass
    return metadata


def process_journal_entries(transactions: pd.DataFrame) -> pd.DataFrame:
    """Group transactions into journal entries and validate balance."""
    journals = transactions.groupby("journal_id")
    processed = pd.DataFrame()
    errors = []

    for journal_id, group in journals:
        if not _validate_journal_balance(group):
            errors.append(journal_id)
            continue

        # Tag each line with metadata summary via iteritems
        for idx, row in group.iterrows():
            meta = _extract_line_metadata(row)
            for key, val in meta.items():
                if key not in group.columns:
                    group.loc[idx, f"meta_{key}"] = val

        processed = processed.append(group, ignore_index=True)

        # Generate reversals for accrual entries
        if (group["journal_type"] == "accrual").any():
            next_month = group["posting_date"].max() + pd.DateOffset(months=1)
            reversal = _build_reversal_entry(group, next_month)
            processed = processed.append(reversal, ignore_index=True)

    if errors:
        console.print(f"[yellow]Skipped {len(errors)} unbalanced journals: "
                      f"{errors[:5]}[/yellow]")

    console.print(f"[green]Processed {processed['journal_id'].nunique()} "
                  f"journal entries ({len(processed)} lines)[/green]")
    return processed
