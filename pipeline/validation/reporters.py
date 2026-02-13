"""Validation result reporting and formatting.

Converts GE validation results into pipeline-friendly formats
for logging, alerting, and deployment gating.
"""

import json
from datetime import datetime
from pathlib import Path

import great_expectations as ge
from great_expectations.core import ExpectationValidationResult
from great_expectations.dataset import PandasDataset
from rich.console import Console
from rich.table import Table

type ReportFormat = str  # "table" | "json" | "summary"

console = Console()


def format_validation_result(
    result: ExpectationValidationResult,
) -> dict[str, str | bool | int]:
    """Extract key fields from a single GE validation result."""
    return {
        "expectation": result.expectation_config.expectation_type,
        "success": result.success,
        "observed_value": str(result.result.get("observed_value", "")),
        "unexpected_count": result.result.get("unexpected_count", 0),
        "element_count": result.result.get("element_count", 0),
    }


def build_validation_report(
    domain: str,
    ge_df: PandasDataset,
    expectations: list[dict],
    output_format: ReportFormat = "table",
) -> str | dict:
    """Run expectations and build a formatted report.

    Executes each expectation against the GE dataset and collects
    results into the requested format.
    """
    results = []
    for exp in expectations:
        method = exp["expectation_type"]
        kwargs = exp.get("kwargs", {})
        try:
            result = getattr(ge_df, method)(**kwargs)
            results.append(format_validation_result(result))
        except Exception as exc:
            results.append({
                "expectation": method,
                "success": False,
                "observed_value": f"ERROR: {exc}",
                "unexpected_count": -1,
                "element_count": 0,
            })

    match output_format:
        case "json":
            return _to_json(domain, results)
        case "summary":
            return _to_summary(domain, results)
        case "table" | _:
            return _to_table(domain, results)


def _to_json(domain: str, results: list[dict]) -> str:
    report = {
        "domain": domain,
        "timestamp": datetime.now().isoformat(),
        "total": len(results),
        "passed": sum(1 for r in results if r["success"]),
        "results": results,
    }
    return json.dumps(report, indent=2)


def _to_summary(domain: str, results: list[dict]) -> str:
    passed = sum(1 for r in results if r["success"])
    total = len(results)
    failed = [r for r in results if not r["success"]]

    lines = [f"[{domain}] {passed}/{total} passed"]
    for f in failed:
        lines.append(f"  FAIL: {f['expectation']} (unexpected: {f['unexpected_count']})")
    return "\n".join(lines)


def _to_table(domain: str, results: list[dict]) -> str:
    table = Table(title=f"Validation: {domain}")
    table.add_column("Expectation", style="cyan")
    table.add_column("Status", style="bold")
    table.add_column("Observed")
    table.add_column("Unexpected", justify="right")

    for r in results:
        status = "[green]PASS[/green]" if r["success"] else "[red]FAIL[/red]"
        table.add_row(
            r["expectation"],
            status,
            str(r["observed_value"]),
            str(r["unexpected_count"]),
        )

    buf = Console(file=None, force_terminal=False)
    with buf.capture() as capture:
        buf.print(table)
    return capture.get()


def save_report(
    report: str | dict,
    output_dir: Path,
    domain: str,
    fmt: ReportFormat = "json",
) -> Path:
    """Persist a validation report to disk."""
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    match fmt:
        case "json":
            path = output_dir / f"{domain}_{timestamp}.json"
            content = report if isinstance(report, str) else json.dumps(report, indent=2)
        case _:
            path = output_dir / f"{domain}_{timestamp}.txt"
            content = str(report)

    path.write_text(content)
    console.print(f"  Report saved: {path}")
    return path
