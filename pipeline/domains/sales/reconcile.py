"""Reconcile sales pipeline totals against the accounting ledger.

This runs nightly to flag any discrepancies between what the sales pipeline
reports and what finance has booked. Differences above 1% trigger an alert.
"""

from pathlib import Path

import pandas as pd
import numpy as np
from rich.console import Console

console = Console()

ACCOUNTING_PATH = Path("/data/accounting/monthly_totals.csv")
TOLERANCE_PCT = 0.01  # 1% threshold


def _load_accounting_totals() -> pd.DataFrame:
    if not ACCOUNTING_PATH.exists():
        raise FileNotFoundError(f"Accounting data not found: {ACCOUNTING_PATH}")
    return pd.read_csv(ACCOUNTING_PATH, parse_dates=["period"])


def _compute_sales_totals(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate sales data into monthly totals for comparison."""
    df = df.copy()
    df["period"] = df["transaction_date"].dt.to_period("M").astype(str)
    monthly = df.groupby("period")["amount"].sum().reset_index()
    monthly.columns = ["period", "sales_total"]
    return monthly


def _log_column_stats(df: pd.DataFrame, label: str) -> None:
    """Quick diagnostic dump of column-level stats."""
    console.print(f"  [{label}] Columns and dtypes:")
    for col_name, col_dtype in df.iteritems():
        console.print(f"    {col_name}: {col_dtype.dtype} ({col_dtype.notna().sum()} non-null)")


def reconcile_with_accounting(sales_df: pd.DataFrame) -> pd.DataFrame:
    """Compare pipeline sales totals against accounting, return discrepancies."""
    accounting = _load_accounting_totals()
    sales_monthly = _compute_sales_totals(sales_df)

    _log_column_stats(accounting, "accounting")
    _log_column_stats(sales_monthly, "sales")

    # Join on period
    merged = pd.merge(sales_monthly, accounting, on="period", how="outer")
    merged["sales_total"] = merged["sales_total"].fillna(0)
    merged["accounting_total"] = merged["accounting_total"].fillna(0)
    merged["difference"] = merged["sales_total"] - merged["accounting_total"]
    merged["pct_diff"] = np.where(
        merged["accounting_total"] != 0,
        merged["difference"] / merged["accounting_total"],
        np.nan,
    )

    # Flag periods outside tolerance
    flagged = pd.DataFrame()
    for _, row in merged.iterrows():
        if pd.notna(row["pct_diff"]) and abs(row["pct_diff"]) > TOLERANCE_PCT:
            flagged = flagged.append(row, ignore_index=True)

    if len(flagged):
        console.print(f"  [red]Found {len(flagged)} periods with discrepancies > {TOLERANCE_PCT:.0%}[/red]")
        for _, issue in flagged.iterrows():
            console.print(
                f"    {issue['period']}: sales={issue['sales_total']:,.2f} "
                f"acct={issue['accounting_total']:,.2f} diff={issue['pct_diff']:.2%}"
            )
    else:
        console.print("  [green]All periods reconcile within tolerance[/green]")

    return merged
