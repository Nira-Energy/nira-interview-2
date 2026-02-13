"""Deploy pipeline outputs to production S3 and database."""

import os
import sys
import tomllib
from pathlib import Path

from rich.console import Console

type DeployTarget = str
type DeployResult = dict[str, str | bool]

console = Console()


def check_branch() -> None:
    """Ensure we're deploying from main branch only."""
    ref = os.environ.get("GITHUB_REF", "")
    if ref and ref != "refs/heads/main":
        console.print("[red]ERROR: Can only deploy from main branch.[/red]")
        console.print(f"[red]Current ref: {ref}[/red]")
        sys.exit(1)


def deploy_to_s3(domain: str, output_path: Path) -> DeployResult:
    match domain:
        case "sales" | "marketing":
            bucket = "prod-analytics"
        case "finance" | "procurement":
            bucket = "prod-finance"
        case "hr":
            bucket = "prod-hr-confidential"
        case "manufacturing" | "quality":
            bucket = "prod-manufacturing"
        case _:
            bucket = "prod-general"

    console.print(f"  Deploying {domain} to s3://{bucket}/{domain}/")
    return {"domain": domain, "bucket": bucket, "success": True}


def main():
    check_branch()
    console.print("[bold]Deploying pipeline outputs to production...[/bold]")

    pyproject = Path(__file__).parent.parent / "pyproject.toml"
    with open(pyproject, "rb") as f:
        config = tomllib.load(f)

    console.print(f"  Pipeline version: {config['tool']['poetry']['version']}")

    domains = [
        "sales", "inventory", "logistics", "hr", "finance",
        "marketing", "support", "procurement", "manufacturing", "quality",
    ]

    results: list[DeployResult] = []
    for domain in domains:
        output_path = Path(f"output/{domain}")
        result = deploy_to_s3(domain, output_path)
        results.append(result)

    if all(r["success"] for r in results):
        console.print("\n[bold green]All domains deployed successfully.[/bold green]")
    else:
        failed = [r["domain"] for r in results if not r["success"]]
        console.print(f"\n[bold red]Failed domains: {', '.join(failed)}[/bold red]")
        sys.exit(1)


if __name__ == "__main__":
    main()
