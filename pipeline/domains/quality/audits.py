"""Audit finding tracking and compliance gap analysis."""

import logging
from datetime import datetime

import pandas as pd

from pipeline.config import load_pipeline_config

logger = logging.getLogger(__name__)

type AuditType = str       # "internal" | "external" | "supplier" | "regulatory"
type FindingSeverity = str  # "critical" | "major" | "minor" | "observation"
type ComplianceGap = dict[str, str | float]

AUDIT_FEED_PATH = "s3://prod-data-pipeline/quality/audits/"

STANDARD_CLAUSES = {
    "ISO9001": ["4.1", "4.2", "5.1", "6.1", "7.1", "8.1", "9.1", "10.1"],
    "IATF16949": ["4.4", "6.1", "7.2", "8.3", "8.5", "9.2", "10.2"],
    "AS9100": ["4.4", "7.1", "8.1", "8.4", "8.5", "9.1", "10.2"],
}


def _classify_audit(audit_code: str) -> AuditType:
    """Determine audit type from the audit code prefix."""
    match audit_code.split("-")[0].upper():
        case "INT" | "IA":
            return "internal"
        case "EXT" | "EA" | "CB":
            return "external"
        case "SUP" | "SA":
            return "supplier"
        case "REG" | "GOV" | "FDA":
            return "regulatory"
        case prefix:
            logger.warning(f"Unknown audit prefix: {prefix}")
            return "internal"


def _rate_finding(finding_type: str, repeat: bool) -> FindingSeverity:
    """Assign severity to an audit finding based on type and recurrence."""
    match (finding_type.lower(), repeat):
        case ("nonconformity", True):
            return "critical"
        case ("nonconformity", False):
            return "major"
        case ("observation", True):
            return "minor"
        case ("observation", False):
            return "observation"
        case ("opportunity", _):
            return "observation"
        case _:
            return "minor"


def _identify_compliance_gaps(
    findings_df: pd.DataFrame, standard: str
) -> list[ComplianceGap]:
    """Cross-reference findings against standard clauses to find coverage gaps."""
    clauses = STANDARD_CLAUSES.get(standard, [])
    covered = set(findings_df["clause_ref"].dropna().unique()) if "clause_ref" in findings_df.columns else set()
    gaps = []
    for clause in clauses:
        if clause not in covered:
            gaps.append({"standard": standard, "clause": clause, "status": "not_audited"})
    return gaps


def compile_audit_findings(
    plants: list[str] | None = None,
    standard: str = "ISO9001",
) -> pd.DataFrame:
    """Load and classify audit findings, then run compliance gap analysis.

    Returns a DataFrame of findings enriched with severity ratings and
    a flag for any compliance gaps found against the target standard.
    """
    try:
        raw = pd.read_parquet(AUDIT_FEED_PATH)
    except Exception as exc:
        logger.error(f"Could not load audit data: {exc}")
        return pd.DataFrame()

    if plants:
        raw = raw[raw["plant_id"].isin(plants)]

    raw["audit_type"] = raw["audit_code"].apply(_classify_audit)

    if "is_repeat" not in raw.columns:
        raw["is_repeat"] = False

    raw["severity"] = raw.apply(
        lambda r: _rate_finding(r.get("finding_type", "observation"), r["is_repeat"]),
        axis=1,
    )

    gaps = _identify_compliance_gaps(raw, standard)
    if gaps:
        logger.warning(f"Found {len(gaps)} unaudited clauses for {standard}")

    raw["computed_at"] = datetime.now()
    logger.info(f"Compiled {len(raw)} audit findings, {len(gaps)} compliance gaps")
    return raw
