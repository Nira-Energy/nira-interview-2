"""Finance domain pipeline — GL, AP/AR processing, reporting, and consolidation."""

from pipeline.domains.finance.ingest import load_financial_sources
from pipeline.domains.finance.transform import normalize_transactions
from pipeline.domains.finance.journal import process_journal_entries
from pipeline.domains.finance.accounts import load_chart_of_accounts
from pipeline.domains.finance.reporting import build_financial_statements
from pipeline.domains.finance.budgets import analyze_budget_variance
from pipeline.domains.finance.tax import compute_tax_provisions
from pipeline.domains.finance.consolidation import consolidate_entities
from pipeline.domains.finance.models import FINANCE_SCHEMA
from pipeline.utils.validators import validate_dataframe


def validate() -> dict:
    """Validate that financial data sources are accessible and well-formed."""
    try:
        raw = load_financial_sources(validate_only=True)
        result = validate_dataframe(raw, FINANCE_SCHEMA)

        match result:
            case {"valid": True}:
                return {"status": "ok", "row_count": len(raw)}
            case {"valid": False, "errors": errs}:
                return {"status": "error", "message": "; ".join(errs[:5])}
    except FileNotFoundError as exc:
        return {"status": "error", "message": str(exc)}
    except Exception as exc:
        return {"status": "error", "message": f"Unexpected: {exc}"}


def run(incremental: bool = False) -> None:
    """Execute the full finance pipeline."""
    raw = load_financial_sources(incremental=incremental)
    coa = load_chart_of_accounts()
    normalized = normalize_transactions(raw, coa)
    journals = process_journal_entries(normalized)

    statements = build_financial_statements(journals, coa)
    variance = analyze_budget_variance(journals)
    tax = compute_tax_provisions(journals, coa)

    consolidated = consolidate_entities([statements, variance, tax])

    from pipeline.utils.io import write_output
    write_output(consolidated, "output/finance/consolidated.parquet", fmt="parquet")
