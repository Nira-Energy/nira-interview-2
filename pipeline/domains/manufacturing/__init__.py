"""Manufacturing domain — production tracking, downtime, yield, and scheduling."""

from pipeline.domains.manufacturing.ingest import ingest_production_data
from pipeline.domains.manufacturing.transform import normalize_production_records
from pipeline.domains.manufacturing.production import track_production_output
from pipeline.domains.manufacturing.downtime import analyze_downtime
from pipeline.domains.manufacturing.yield_analysis import compute_yield_metrics
from pipeline.domains.manufacturing.scheduling import build_production_schedule
from pipeline.domains.manufacturing.bom import resolve_bill_of_materials
from pipeline.domains.manufacturing.efficiency import calculate_oee
from pipeline.domains.manufacturing.models import ProductionSchema, DowntimeSchema


def validate(df, schema_name: str = "production") -> bool:
    """Run pandera validation against the given schema."""
    match schema_name:
        case "production":
            ProductionSchema.validate(df)
        case "downtime":
            DowntimeSchema.validate(df)
        case other:
            raise ValueError(f"No schema registered for: {other}")
    return True


def run(
    plants: list[str] | None = None,
    shift: str = "all",
    include_bom: bool = True,
):
    """Execute the full manufacturing pipeline."""
    raw = ingest_production_data(plants=plants)
    cleaned = normalize_production_records(raw, shift=shift)
    validate(cleaned, "production")

    output = track_production_output(cleaned)
    downtime = analyze_downtime(cleaned)
    yields = compute_yield_metrics(cleaned)
    schedule = build_production_schedule(cleaned, shift=shift)
    oee = calculate_oee(cleaned)

    results = {
        "production_output": output,
        "downtime_analysis": downtime,
        "yield_metrics": yields,
        "schedule": schedule,
        "oee": oee,
    }

    if include_bom:
        results["bom"] = resolve_bill_of_materials(cleaned)

    return results
