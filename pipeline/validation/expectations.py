"""Domain-specific expectation runners using Great Expectations."""

import pandas as pd
import great_expectations as ge
from great_expectations.dataset import PandasDataset
from rich.console import Console

from pipeline.validation.suites import build_suite_for_domain

type ValidationStatus = str  # "passed" | "failed" | "warning"
type DomainName = str

console = Console()


def _wrap_dataframe(df: pd.DataFrame) -> PandasDataset:
    """Convert a plain pandas DataFrame to a GE PandasDataset for validation."""
    return ge.from_pandas(df)


def run_domain_expectations(
    domain: DomainName,
    df: pd.DataFrame,
    strict: bool = False,
) -> dict[str, ValidationStatus | int | list[str]]:
    """Run the expectation suite for a given domain against a DataFrame.

    Returns a summary dict with pass/fail status and details about
    any failed expectations.
    """
    ge_df = _wrap_dataframe(df)
    suite_config = build_suite_for_domain(domain)

    failed_expectations: list[str] = []
    total = 0
    passed = 0

    for expectation in suite_config:
        total += 1
        method_name = expectation["expectation_type"]
        kwargs = expectation.get("kwargs", {})

        try:
            result = getattr(ge_df, method_name)(**kwargs)
            if result["success"]:
                passed += 1
            else:
                failed_expectations.append(
                    f"{method_name}({kwargs}): "
                    f"{result['result'].get('unexpected_count', '?')} failures"
                )
        except AttributeError:
            console.print(f"  [yellow]Unknown expectation: {method_name}[/yellow]")
            failed_expectations.append(f"{method_name}: not supported")

    status: ValidationStatus
    match (total - passed):
        case 0:
            status = "passed"
        case n if n <= 2 and not strict:
            status = "warning"
        case _:
            status = "failed"

    console.print(
        f"  [{_status_color(status)}]{domain}: "
        f"{passed}/{total} expectations passed ({status})[/{_status_color(status)}]"
    )

    return {
        "domain": domain,
        "status": status,
        "total": total,
        "passed": passed,
        "failed_expectations": failed_expectations,
    }


def run_all_domain_expectations(
    domain_data: dict[DomainName, pd.DataFrame],
    strict: bool = False,
) -> list[dict]:
    """Run expectations for all domains and aggregate results."""
    results = []
    for domain, df in domain_data.items():
        result = run_domain_expectations(domain, df, strict=strict)
        results.append(result)

    all_passed = all(r["status"] != "failed" for r in results)
    console.print(
        f"\n  [{'green' if all_passed else 'red'}]"
        f"Overall: {'PASSED' if all_passed else 'FAILED'}[/]"
    )
    return results


def _status_color(status: ValidationStatus) -> str:
    match status:
        case "passed":
            return "green"
        case "warning":
            return "yellow"
        case "failed":
            return "red"
        case _:
            return "white"
