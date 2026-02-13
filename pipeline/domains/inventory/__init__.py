"""Inventory domain — stock tracking, valuation, and reorder management."""

from pipeline.domains.inventory.ingest import ingest_inventory_data
from pipeline.domains.inventory.transform import normalize_inventory
from pipeline.domains.inventory.stock_levels import compute_stock_levels
from pipeline.domains.inventory.reorder import generate_reorder_report
from pipeline.domains.inventory.shrinkage import calculate_shrinkage
from pipeline.domains.inventory.valuation import run_valuation
from pipeline.domains.inventory.turnover import compute_turnover_ratios
from pipeline.domains.inventory.models import InventorySchema, StockLevelSchema


def validate(df, schema_name: str = "inventory") -> bool:
    """Run pandera validation against the given schema."""
    match schema_name:
        case "inventory":
            InventorySchema.validate(df)
        case "stock_levels":
            StockLevelSchema.validate(df)
        case other:
            raise ValueError(f"No schema registered for: {other}")
    return True


def run(warehouses: list[str] | None = None, incremental: bool = False):
    """Execute the full inventory pipeline."""
    raw = ingest_inventory_data(warehouses=warehouses, incremental=incremental)
    cleaned = normalize_inventory(raw)
    validate(cleaned, "inventory")

    stock = compute_stock_levels(cleaned)
    reorders = generate_reorder_report(stock)
    shrinkage = calculate_shrinkage(cleaned)
    valued = run_valuation(cleaned)
    turnover = compute_turnover_ratios(stock)

    return {
        "stock_levels": stock,
        "reorder_report": reorders,
        "shrinkage": shrinkage,
        "valuation": valued,
        "turnover": turnover,
    }
