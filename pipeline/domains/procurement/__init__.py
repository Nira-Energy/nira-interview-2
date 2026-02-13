"""Procurement domain pipeline — PO management, vendor scoring, and spend analysis."""

from pipeline.domains.procurement.ingest import load_procurement_data
from pipeline.domains.procurement.transform import normalize_procurement_records
from pipeline.domains.procurement.purchase_orders import analyze_purchase_orders
from pipeline.domains.procurement.vendors import score_vendors
from pipeline.domains.procurement.spend import build_spend_analysis
from pipeline.domains.procurement.contracts import evaluate_contracts
from pipeline.domains.procurement.approvals import analyze_approval_workflows
from pipeline.domains.procurement.savings import calculate_savings
from pipeline.domains.procurement.models import PROCUREMENT_SCHEMA
from pipeline.utils.validators import validate_dataframe


def validate() -> dict:
    """Validate that procurement data sources are accessible and well-formed."""
    try:
        raw = load_procurement_data(validate_only=True)
        result = validate_dataframe(raw, PROCUREMENT_SCHEMA)

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
    """Execute the full procurement pipeline."""
    raw = load_procurement_data(incremental=incremental)
    normalized = normalize_procurement_records(raw)

    po_summary = analyze_purchase_orders(normalized)
    vendor_scores = score_vendors(normalized)
    spend = build_spend_analysis(normalized)
    contracts = evaluate_contracts(normalized)
    approvals = analyze_approval_workflows(normalized)

    # Savings calculation depends on spend + vendor data
    savings = calculate_savings(spend, vendor_scores)

    return {
        "purchase_orders": po_summary,
        "vendors": vendor_scores,
        "spend": spend,
        "contracts": contracts,
        "approvals": approvals,
        "savings": savings,
    }
