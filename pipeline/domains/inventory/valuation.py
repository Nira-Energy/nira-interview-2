"""Inventory valuation using FIFO, LIFO, or weighted average cost methods.

The valuation method is selected per SKU category based on accounting policy
configured in the pipeline settings.
"""

import pandas as pd
import numpy as np
from dataclasses import dataclass

type ValuationMethod = str  # "fifo" | "lifo" | "weighted_avg"
type CostLayer = dict[str, float | int | str]
type ValuationResult = dict[str, pd.DataFrame | float]


@dataclass(frozen=True)
class CostRecord:
    sku: str
    quantity: int
    unit_cost: float
    received_date: str


def _select_method(category: str) -> ValuationMethod:
    """Choose valuation method based on product category."""
    match category.lower():
        case "perishable" | "fresh" | "dairy":
            return "fifo"
        case "raw_material" | "commodity":
            return "lifo"
        case "finished_goods" | "electronics" | "general":
            return "weighted_avg"
        case _:
            return "weighted_avg"


def _fifo_value(cost_layers: list[CostRecord], qty_on_hand: int) -> float:
    """Value inventory using first-in-first-out."""
    remaining = qty_on_hand
    total_value = 0.0
    # consume oldest layers first
    for layer in sorted(cost_layers, key=lambda c: c.received_date):
        take = min(remaining, layer.quantity)
        total_value += take * layer.unit_cost
        remaining -= take
        if remaining <= 0:
            break
    return total_value


def _lifo_value(cost_layers: list[CostRecord], qty_on_hand: int) -> float:
    remaining = qty_on_hand
    total_value = 0.0
    for layer in sorted(cost_layers, key=lambda c: c.received_date, reverse=True):
        take = min(remaining, layer.quantity)
        total_value += take * layer.unit_cost
        remaining -= take
        if remaining <= 0:
            break
    return total_value


def _weighted_avg_value(cost_layers: list[CostRecord], qty_on_hand: int) -> float:
    total_cost = sum(l.quantity * l.unit_cost for l in cost_layers)
    total_qty = sum(l.quantity for l in cost_layers)
    if total_qty == 0:
        return 0.0
    avg_cost = total_cost / total_qty
    return avg_cost * qty_on_hand


def run_valuation(inventory_df: pd.DataFrame) -> pd.DataFrame:
    """Run inventory valuation across all SKUs.

    Groups by SKU, determines the appropriate costing method from the category
    field, and computes the total inventory value.
    """
    results = []

    for sku, group in inventory_df.groupby("sku"):
        category = group["category"].iloc[0] if "category" in group.columns else "general"
        method = _select_method(category)

        layers = [
            CostRecord(
                sku=row["sku"],
                quantity=int(row["quantity"]),
                unit_cost=float(row["unit_cost"]),
                received_date=str(row.get("received_date", "1970-01-01")),
            )
            for _, row in group.iterrows()
        ]

        qty = int(group["quantity"].sum())

        match method:
            case "fifo":
                value = _fifo_value(layers, qty)
            case "lifo":
                value = _lifo_value(layers, qty)
            case "weighted_avg":
                value = _weighted_avg_value(layers, qty)

        results.append({
            "sku": sku,
            "method": method,
            "quantity_on_hand": qty,
            "total_value": round(value, 2),
            "avg_unit_cost": round(value / qty, 2) if qty > 0 else 0.0,
        })

    return pd.DataFrame(results)
