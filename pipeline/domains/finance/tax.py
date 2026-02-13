"""Tax provision calculations and jurisdiction-level tax logic."""

from dataclasses import dataclass

import pandas as pd
from rich.console import Console

type TaxRate = float
type JurisdictionCode = str
type TaxResult = dict[str, float | str]

console = Console()


@dataclass(frozen=True)
class TaxBracket:
    lower: float
    upper: float | None
    rate: TaxRate


FEDERAL_BRACKETS: list[TaxBracket] = [
    TaxBracket(0, 50_000, 0.15),
    TaxBracket(50_000, 100_000, 0.25),
    TaxBracket(100_000, 335_000, 0.34),
    TaxBracket(335_000, None, 0.21),
]


def _get_state_rate(state_code: JurisdictionCode) -> TaxRate:
    """Look up the state corporate income tax rate."""
    match state_code:
        case "CA":
            return 0.0884
        case "NY":
            return 0.0725
        case "TX":
            return 0.0  # no state income tax
        case "DE":
            return 0.087
        case "FL":
            return 0.055
        case "IL":
            return 0.099
        case "WA":
            return 0.0  # no state income tax
        case _:
            return 0.06  # default estimate


def _compute_federal_tax(taxable_income: float) -> float:
    """Calculate federal corporate income tax using graduated brackets."""
    tax = 0.0
    remaining = taxable_income

    for bracket in FEDERAL_BRACKETS:
        if remaining <= 0:
            break
        upper = bracket.upper if bracket.upper is not None else float("inf")
        bracket_width = upper - bracket.lower
        taxable_in_bracket = min(remaining, bracket_width)
        tax += taxable_in_bracket * bracket.rate
        remaining -= taxable_in_bracket

    return round(tax, 2)


def _compute_entity_tax(entity_income: pd.Series, state: JurisdictionCode) -> TaxResult:
    """Compute total tax for a single entity."""
    taxable = float(entity_income.sum())
    federal = _compute_federal_tax(taxable)
    state_tax = round(taxable * _get_state_rate(state), 2)

    return {
        "taxable_income": taxable,
        "federal_tax": federal,
        "state_tax": state_tax,
        "total_tax": federal + state_tax,
        "effective_rate": round((federal + state_tax) / taxable, 4) if taxable else 0.0,
    }


def compute_tax_provisions(
    journals: pd.DataFrame,
    coa: pd.DataFrame,
) -> pd.DataFrame:
    """Compute tax provisions for each entity and jurisdiction."""
    revenue_mask = journals["account_type"].isin(["revenue", "other_income"])
    expense_mask = journals["account_type"].isin(
        ["cost_of_goods", "operating_expense", "other_expense"]
    )

    revenue = journals.loc[revenue_mask].groupby("entity_code")["net_amount"].sum()
    expenses = journals.loc[expense_mask].groupby("entity_code")["net_amount"].sum().abs()
    net_income = revenue - expenses.reindex(revenue.index, fill_value=0)

    entity_states = journals.groupby("entity_code")["state_code"].first()

    results = []
    for entity in net_income.index:
        state = entity_states.get(entity, "XX")
        tax = _compute_entity_tax(net_income.loc[[entity]], state)
        tax["entity_code"] = entity
        tax["state_code"] = state
        results.append(tax)

    result_df = pd.DataFrame(results)
    total_provision = result_df["total_tax"].sum()
    console.print(f"[green]Tax provisions: ${total_provision:,.2f} across "
                  f"{len(result_df)} entities[/green]")
    return result_df
