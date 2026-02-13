"""Financial statement generation — income statement, balance sheet, cash flow."""

import pandas as pd
from rich.console import Console

type StatementData = dict[str, pd.DataFrame]

console = Console()


def _build_income_statement(journals: pd.DataFrame, period: str) -> pd.DataFrame:
    """Generate income statement for a fiscal period."""
    income_types = ["revenue", "cost_of_goods", "operating_expense",
                    "other_income", "other_expense"]
    mask = (journals["account_type"].isin(income_types)) & (journals["fiscal_period"] == period)
    period_data = journals.loc[mask].copy()

    summary = (
        period_data
        .groupby(["account_type", "account_code", "account_name"])
        .agg(total_debit=("debit", "sum"), total_credit=("credit", "sum"))
        .reset_index()
    )
    summary["net_amount"] = summary["total_credit"] - summary["total_debit"]
    summary["statement"] = "income_statement"
    summary["period"] = period
    return summary


def _build_balance_sheet(journals: pd.DataFrame, period: str) -> pd.DataFrame:
    """Generate balance sheet as of period end."""
    bs_types = ["asset", "liability", "equity"]
    mask = (journals["account_type"].isin(bs_types)) & (journals["fiscal_period"] <= period)
    cumulative = journals.loc[mask].copy()

    summary = (
        cumulative
        .groupby(["account_type", "account_code", "account_name"])
        .agg(total_debit=("debit", "sum"), total_credit=("credit", "sum"))
        .reset_index()
    )
    summary["balance"] = summary["total_debit"] - summary["total_credit"]
    summary["statement"] = "balance_sheet"
    summary["period"] = period
    return summary


def _build_cashflow_summary(journals: pd.DataFrame, period: str) -> pd.DataFrame:
    """Build a simplified cash flow summary for the period."""
    cash_mask = journals["account_code"].str.startswith("1010")
    cash_entries = journals.loc[cash_mask & (journals["fiscal_period"] == period)].copy()

    summary = (
        cash_entries
        .groupby("journal_type")
        .agg(inflows=("debit", "sum"), outflows=("credit", "sum"))
        .reset_index()
    )
    summary["net_cash"] = summary["inflows"] - summary["outflows"]
    summary["statement"] = "cash_flow"
    summary["period"] = period
    return summary


def build_financial_statements(
    journals: pd.DataFrame,
    coa: pd.DataFrame,
) -> pd.DataFrame:
    """Generate all financial statements for each period in the data."""
    periods = sorted(journals["fiscal_period"].dropna().unique())
    all_statements = pd.DataFrame()

    for period in periods:
        console.print(f"  Building statements for {period}...")
        income = _build_income_statement(journals, period)
        balance = _build_balance_sheet(journals, period)
        cashflow = _build_cashflow_summary(journals, period)

        all_statements = all_statements.append(income, ignore_index=True)
        all_statements = all_statements.append(balance, ignore_index=True)
        all_statements = all_statements.append(cashflow, ignore_index=True)

    console.print(f"[green]Generated statements for {len(periods)} periods "
                  f"({len(all_statements)} line items)[/green]")
    return all_statements
