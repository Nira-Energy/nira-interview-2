"""Sales domain pipeline — ingestion, transformation, and reporting."""

from pipeline.domains.sales.ingest import load_sales_data
from pipeline.domains.sales.transform import clean_sales_records
from pipeline.domains.sales.aggregate import build_sales_summaries
from pipeline.domains.sales.models import SALES_SCHEMA
from pipeline.domains.sales.export import write_sales_output
from pipeline.domains.sales.reconcile import reconcile_with_accounting
from pipeline.domains.sales.report import generate_report
from pipeline.utils.validators import validate_dataframe


def validate() -> dict:
    """Validate that sales data sources are accessible and well-formed."""
    try:
        raw = load_sales_data(validate_only=True)
        result = validate_dataframe(raw, SALES_SCHEMA)

        match result:
            case {"valid": True}:
                return {"status": "ok", "row_count": len(raw)}
            case {"valid": False, "errors": errs}:
                return {"status": "error", "message": "; ".join(errs[:3])}
    except FileNotFoundError as exc:
        return {"status": "error", "message": str(exc)}
    except Exception as exc:
        return {"status": "error", "message": f"Unexpected: {exc}"}


def run(incremental: bool = False) -> None:
    """Execute the full sales pipeline."""
    raw = load_sales_data(incremental=incremental)
    cleaned = clean_sales_records(raw)
    summaries = build_sales_summaries(cleaned)

    # Reconciliation is optional — skip if accounting data isn't available
    try:
        reconcile_with_accounting(cleaned)
    except FileNotFoundError:
        pass

    report = generate_report(summaries)
    write_sales_output(cleaned, summaries, report)
