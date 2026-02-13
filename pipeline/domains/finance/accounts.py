"""Chart of accounts management and account hierarchy."""

from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path

import pandas as pd
from rich.console import Console

type AccountCode = str
type AccountTree = dict[str, list["AccountNode"]]
type BalanceMap = dict[AccountCode, float]

console = Console()

COA_PATH = Path("data/finance/chart_of_accounts.csv")


class AccountCategory(StrEnum):
    ASSET = "asset"
    LIABILITY = "liability"
    EQUITY = "equity"
    REVENUE = "revenue"
    EXPENSE = "expense"


@dataclass
class AccountNode:
    code: AccountCode
    name: str
    category: AccountCategory
    parent_code: AccountCode | None
    is_header: bool = False


def _classify_category(code: AccountCode) -> AccountCategory:
    """Determine account category from the code prefix."""
    match code[:1]:
        case "1":
            return AccountCategory.ASSET
        case "2":
            return AccountCategory.LIABILITY
        case "3":
            return AccountCategory.EQUITY
        case "4":
            return AccountCategory.REVENUE
        case "5" | "6" | "7" | "8":
            return AccountCategory.EXPENSE
        case _:
            return AccountCategory.EXPENSE


def _get_normal_balance(category: AccountCategory) -> str:
    """Return whether the account normally carries a debit or credit balance."""
    match category:
        case AccountCategory.ASSET | AccountCategory.EXPENSE:
            return "debit"
        case AccountCategory.LIABILITY | AccountCategory.EQUITY | AccountCategory.REVENUE:
            return "credit"


def build_account_tree(coa_df: pd.DataFrame) -> AccountTree:
    """Build a hierarchical tree of accounts grouped by category."""
    tree: AccountTree = {}
    for _, row in coa_df.iterrows():
        cat = _classify_category(row["account_code"])
        node = AccountNode(
            code=row["account_code"],
            name=row["account_name"],
            category=cat,
            parent_code=row.get("parent_code"),
            is_header=row.get("is_header", False),
        )
        tree.setdefault(cat.value, []).append(node)
    return tree


def load_chart_of_accounts() -> pd.DataFrame:
    """Load and enrich the chart of accounts."""
    coa = pd.read_csv(COA_PATH)
    coa["category"] = coa["account_code"].apply(_classify_category)
    coa["normal_balance"] = coa["category"].apply(_get_normal_balance)

    console.print(f"[green]Loaded {len(coa)} accounts across "
                  f"{coa['category'].nunique()} categories[/green]")
    return coa
