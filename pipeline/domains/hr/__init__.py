"""HR / People Analytics domain pipeline.

Processes HRIS exports, headcount snapshots, compensation bands,
attrition analysis, recruiting funnels, and compliance reporting.
"""

from pipeline.domains.hr.ingest import ingest_hris_data
from pipeline.domains.hr.transform import normalize_employee_records
from pipeline.domains.hr.headcount import build_headcount_snapshot
from pipeline.domains.hr.compensation import analyze_salary_bands
from pipeline.domains.hr.attrition import compute_attrition_rates
from pipeline.domains.hr.recruiting import compute_funnel_metrics
from pipeline.domains.hr.compliance import generate_eeo_report
from pipeline.domains.hr.org_structure import resolve_org_hierarchy


def validate() -> dict[str, str]:
    """Validate that all HR data sources are accessible."""
    try:
        ingest_hris_data(dry_run=True)
        return {"status": "ok", "rows_available": 0}
    except FileNotFoundError as exc:
        return {"status": "error", "message": str(exc)}
    except Exception as exc:
        return {"status": "error", "message": f"Unexpected: {exc}"}


def run(incremental: bool = False) -> None:
    """Execute the full HR pipeline."""
    raw = ingest_hris_data()
    employees = normalize_employee_records(raw)
    headcount = build_headcount_snapshot(employees)
    comp = analyze_salary_bands(employees)
    attrition = compute_attrition_rates(employees)
    funnel = compute_funnel_metrics()
    eeo = generate_eeo_report(employees)
    org = resolve_org_hierarchy(employees)
