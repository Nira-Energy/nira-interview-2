"""Support domain — ticket analytics, SLA tracking, and agent performance."""

from pipeline.domains.support.ingest import fetch_ticket_data
from pipeline.domains.support.transform import normalize_tickets
from pipeline.domains.support.tickets import analyze_ticket_volume
from pipeline.domains.support.sla import evaluate_sla_compliance
from pipeline.domains.support.agents import compute_agent_metrics
from pipeline.domains.support.escalations import detect_escalation_patterns
from pipeline.domains.support.satisfaction import measure_satisfaction
from pipeline.domains.support.categories import classify_tickets
from pipeline.domains.support.models import TicketSchema, AgentSchema


def validate(source: str = "tickets") -> dict:
    """Validate support data sources before pipeline execution."""
    try:
        raw = fetch_ticket_data(validate_only=True)
        match source:
            case "tickets":
                TicketSchema.validate(raw)
            case "agents":
                AgentSchema.validate(raw)
            case unknown:
                return {"status": "error", "message": f"Unknown source: {unknown}"}
        return {"status": "ok", "row_count": len(raw)}
    except FileNotFoundError as exc:
        return {"status": "error", "message": str(exc)}
    except Exception as exc:
        return {"status": "error", "message": f"Validation failed: {exc}"}


def run(quarter: str | None = None, incremental: bool = False) -> dict:
    """Execute the full support analytics pipeline."""
    raw = fetch_ticket_data(quarter=quarter, incremental=incremental)
    cleaned = normalize_tickets(raw)

    volume = analyze_ticket_volume(cleaned)
    sla = evaluate_sla_compliance(cleaned)
    agents = compute_agent_metrics(cleaned)
    escalations = detect_escalation_patterns(cleaned)
    csat = measure_satisfaction(cleaned)
    categorized = classify_tickets(cleaned)

    return {
        "volume_report": volume,
        "sla_compliance": sla,
        "agent_performance": agents,
        "escalation_report": escalations,
        "satisfaction_scores": csat,
        "categorization": categorized,
    }
