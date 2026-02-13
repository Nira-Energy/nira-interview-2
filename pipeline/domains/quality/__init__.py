"""Quality domain — inspection management, defect tracking, and compliance."""

from pipeline.domains.quality.ingest import ingest_inspection_data
from pipeline.domains.quality.transform import normalize_inspections
from pipeline.domains.quality.inspections import track_inspection_results
from pipeline.domains.quality.defects import analyze_defect_trends
from pipeline.domains.quality.ncr import process_nonconformance_reports
from pipeline.domains.quality.audits import compile_audit_findings
from pipeline.domains.quality.metrics import compute_quality_kpis
from pipeline.domains.quality.corrective_actions import track_capa_status
from pipeline.domains.quality.models import InspectionSchema, DefectSchema


def validate(df, schema_name: str = "inspection") -> bool:
    """Run pandera validation against the named quality schema."""
    match schema_name:
        case "inspection":
            InspectionSchema.validate(df)
        case "defect":
            DefectSchema.validate(df)
        case other:
            raise ValueError(f"No schema registered for: {other}")
    return True


def run(
    plants: list[str] | None = None,
    include_ncr: bool = True,
    lookback_days: int = 90,
):
    """Execute the full quality pipeline for one or more plants."""
    raw = ingest_inspection_data(plants=plants, lookback_days=lookback_days)
    cleaned = normalize_inspections(raw)
    validate(cleaned, "inspection")

    results = track_inspection_results(cleaned)
    defects = analyze_defect_trends(results)
    kpis = compute_quality_kpis(results, defects)
    audits = compile_audit_findings(plants=plants)
    capas = track_capa_status(defects)

    output = {
        "inspection_results": results,
        "defect_trends": defects,
        "quality_kpis": kpis,
        "audit_findings": audits,
        "corrective_actions": capas,
    }

    if include_ncr:
        output["ncr_report"] = process_nonconformance_reports(cleaned)

    return output
