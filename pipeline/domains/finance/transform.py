"""Transaction normalization and classification for financial data."""

import pandas as pd
from rich.console import Console

type AccountCode = str
type AmountCents = int

console = Console()

DEBIT_CREDIT_THRESHOLD = 0.01


def classify_account_type(account_code: AccountCode) -> str:
    """Map an account code prefix to its financial statement category."""
    prefix = account_code[:1] if account_code else ""

    match prefix:
        case "1":
            return "asset"
        case "2":
            return "liability"
        case "3":
            return "equity"
        case "4":
            return "revenue"
        case "5":
            return "cost_of_goods"
        case "6":
            return "operating_expense"
        case "7":
            return "other_income"
        case "8":
            return "other_expense"
        case "9":
            return "intercompany"
        case _:
            return "unclassified"


def _normalize_amounts(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure debit/credit columns are consistent and balanced."""
    df["debit"] = df["debit"].fillna(0.0).round(2)
    df["credit"] = df["credit"].fillna(0.0).round(2)
    df["net_amount"] = df["debit"] - df["credit"]

    imbalance = df.groupby("journal_id")["net_amount"].sum()
    unbalanced = imbalance[imbalance.abs() > DEBIT_CREDIT_THRESHOLD]

    if not unbalanced.empty:
        console.print(f"[yellow]Warning: {len(unbalanced)} unbalanced journals[/yellow]")

    return df


def _apply_period_logic(row: pd.Series) -> str:
    """Determine the fiscal period for a transaction."""
    match row.get("adjustment_type"):
        case "prior_period":
            return row["original_period"]
        case "accrual_reversal":
            return row["reversal_period"]
        case "reclassification":
            return row["posting_period"]
        case None | "standard":
            return row["posting_period"]
        case other:
            console.print(f"[red]Unknown adjustment type: {other}[/red]")
            return row["posting_period"]


def normalize_transactions(raw: pd.DataFrame, coa: pd.DataFrame) -> pd.DataFrame:
    """Normalize raw financial data with account classification and balancing."""
    df = raw.copy()
    df = df.merge(coa[["account_code", "account_name"]], on="account_code", how="left")
    df["account_type"] = df["account_code"].apply(classify_account_type)
    df = _normalize_amounts(df)
    df["fiscal_period"] = df.apply(_apply_period_logic, axis=1)

    console.print(f"[green]Normalized {len(df)} transactions across "
                  f"{df['account_type'].nunique()} account types[/green]")
    return df
