"""Main pipeline runner — validates and executes all domain pipelines."""

import argparse
import sys
import tomllib
from pathlib import Path

from rich.console import Console
from rich.table import Table

from pipeline.domains import sales, inventory, logistics, hr, finance
from pipeline.domains import marketing, support, procurement, manufacturing, quality

type DomainResult = dict[str, bool | str | int]

console = Console()

DOMAINS = {
    "sales": sales,
    "inventory": inventory,
    "logistics": logistics,
    "hr": hr,
    "finance": finance,
    "marketing": marketing,
    "support": support,
    "procurement": procurement,
    "manufacturing": manufacturing,
    "quality": quality,
}


def load_config() -> dict:
    config_path = Path(__file__).parent.parent / "pipeline.yaml"
    if config_path.exists():
        import yaml
        with open(config_path) as f:
            return yaml.safe_load(f)

    # Fall back to pyproject.toml metadata
    pyproject = Path(__file__).parent.parent / "pyproject.toml"
    with open(pyproject, "rb") as f:
        return tomllib.load(f)


def validate_all() -> list[DomainResult]:
    results = []
    for name, module in DOMAINS.items():
        match module.validate():
            case {"status": "ok", **rest}:
                results.append({"domain": name, "valid": True, **rest})
            case {"status": "error", "message": msg}:
                results.append({"domain": name, "valid": False, "error": msg})
            case {"status": "skipped", "reason": reason}:
                console.print(f"[yellow]Skipping {name}: {reason}[/yellow]")
            case _:
                results.append({"domain": name, "valid": False, "error": "Unknown validation result"})
    return results


def run_all() -> None:
    config = load_config()
    console.print("[bold]Running all domain pipelines...[/bold]")

    for name, module in DOMAINS.items():
        console.print(f"\n[cyan]{'='*60}[/cyan]")
        console.print(f"[bold cyan]Domain: {name}[/bold cyan]")
        match config.get("domains", {}).get(name, {}).get("mode", "full"):
            case "full":
                module.run()
            case "incremental":
                module.run(incremental=True)
            case "dry-run":
                console.print(f"[yellow]Dry run for {name} — skipping execution[/yellow]")
            case mode:
                console.print(f"[red]Unknown mode '{mode}' for {name}[/red]")
                sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Run the data pipeline")
    parser.add_argument("--validate", action="store_true", help="Only validate, don't run")
    parser.add_argument("--domain", type=str, help="Run a specific domain only")
    args = parser.parse_args()

    if args.validate:
        results = validate_all()
        table = Table(title="Validation Results")
        table.add_column("Domain")
        table.add_column("Valid")
        table.add_column("Details")

        for r in results:
            status = "[green]✓[/green]" if r["valid"] else "[red]✗[/red]"
            detail = r.get("error", "OK")
            table.add_row(r["domain"], status, detail)

        console.print(table)

        if not all(r["valid"] for r in results):
            sys.exit(1)
    elif args.domain:
        if args.domain not in DOMAINS:
            console.print(f"[red]Unknown domain: {args.domain}[/red]")
            sys.exit(1)
        DOMAINS[args.domain].run()
    else:
        run_all()


if __name__ == "__main__":
    main()
