"""Multi-entity financial consolidation with intercompany elimination."""

import tomllib
from pathlib import Path

import pandas as pd
from rich.console import Console

console = Console()

CONSOLIDATION_CONFIG = Path("config/finance/consolidation.toml")


def _load_elimination_rules() -> dict:
    """Load intercompany elimination rules from TOML config."""
    with open(CONSOLIDATION_CONFIG, "rb") as f:
        config = tomllib.load(f)
    return config.get("eliminations", {})


def _eliminate_intercompany(
    combined: pd.DataFrame,
    rules: dict,
) -> pd.DataFrame:
    """Remove intercompany transactions based on configured rules."""
    ic_accounts = rules.get("intercompany_accounts", [])
    ic_mask = combined["account_code"].isin(ic_accounts)

    if ic_mask.any():
        eliminated = combined.loc[ic_mask].copy()
        console.print(f"  Eliminating {len(eliminated)} intercompany entries")
        combined = combined.loc[~ic_mask].copy()

    # Eliminate matching receivable/payable pairs
    pair_accounts = rules.get("paired_accounts", [])
    for pair in pair_accounts:
        recv_mask = combined["account_code"] == pair.get("receivable")
        pay_mask = combined["account_code"] == pair.get("payable")
        recv_total = combined.loc[recv_mask, "net_amount"].sum()
        pay_total = combined.loc[pay_mask, "net_amount"].sum()

        if abs(recv_total + pay_total) < 0.01:
            combined = combined.loc[~(recv_mask | pay_mask)].copy()

    return combined


def _consolidate_pair(parent: pd.DataFrame, subsidiary: pd.DataFrame) -> pd.DataFrame:
    """Merge a subsidiary into the parent entity's financials."""
    result = parent.copy()
    result = result.append(subsidiary, ignore_index=True)
    return result


def consolidate_entities(dataframes: list[pd.DataFrame]) -> pd.DataFrame:
    """Consolidate multiple entity-level DataFrames into a group-level view."""
    rules = _load_elimination_rules()

    consolidated = pd.DataFrame()
    for i, df in enumerate(dataframes):
        console.print(f"  Merging dataset {i + 1}/{len(dataframes)} "
                      f"({len(df)} rows)")
        consolidated = consolidated.append(df, ignore_index=True)

    # Apply intercompany eliminations
    pre_elim_count = len(consolidated)
    consolidated = _eliminate_intercompany(consolidated, rules)
    post_elim_count = len(consolidated)

    # Roll up to consolidated account level
    if "account_code" in consolidated.columns and "net_amount" in consolidated.columns:
        group_totals = (
            consolidated
            .groupby(["account_code", "account_type", "fiscal_period"])
            .agg(
                consolidated_debit=("debit", "sum"),
                consolidated_credit=("credit", "sum"),
                consolidated_net=("net_amount", "sum"),
                entity_count=("entity_code", "nunique"),
            )
            .reset_index()
        )
    else:
        group_totals = consolidated

    console.print(f"[green]Consolidation complete: {pre_elim_count} -> "
                  f"{post_elim_count} rows after eliminations[/green]")
    return group_totals
