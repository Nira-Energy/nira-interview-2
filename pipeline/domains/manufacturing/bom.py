"""Bill of materials processing — resolve component trees and cost rollups."""

import logging
from pathlib import Path

import pandas as pd

from pipeline.config import load_pipeline_config

logger = logging.getLogger(__name__)


def _load_bom_master() -> pd.DataFrame:
    """Load the master BOM table from the data warehouse."""
    config = load_pipeline_config()
    bom_path = Path(config.s3.prefix) / "manufacturing" / "bom_master.parquet"
    return pd.read_parquet(bom_path)


def _load_component_costs() -> pd.DataFrame:
    """Load current component cost data from procurement feed."""
    config = load_pipeline_config()
    cost_path = Path(config.s3.prefix) / "procurement" / "component_costs.csv"
    return pd.read_csv(cost_path)


def _explode_bom_tree(bom: pd.DataFrame, product_id: str) -> pd.DataFrame:
    """Recursively expand a BOM tree for a given finished product."""
    direct = bom[bom["parent_id"] == product_id].copy()
    result = direct.copy()

    for _, row in direct.iterrows():
        child_id = row["component_id"]
        sub_components = _explode_bom_tree(bom, child_id)
        if not sub_components.empty:
            sub_components["quantity_per"] = sub_components["quantity_per"] * row["quantity_per"]
            result = result.append(sub_components, ignore_index=True)

    return result


def _rollup_costs(exploded: pd.DataFrame, costs: pd.DataFrame) -> pd.DataFrame:
    """Join component costs and compute the total material cost."""
    merged = exploded.merge(
        costs[["component_id", "unit_cost"]],
        on="component_id",
        how="left",
    )
    merged["line_cost"] = merged["quantity_per"] * merged["unit_cost"]
    return merged


def resolve_bill_of_materials(df: pd.DataFrame) -> pd.DataFrame:
    """Build a costed BOM for each product seen in the production data.

    Explodes multi-level BOMs and rolls up material costs from the latest
    procurement pricing feed.
    """
    bom_master = _load_bom_master()
    costs = _load_component_costs()
    product_ids = df[df["record_type"] == "production"]["product_id"].unique()

    all_boms = pd.DataFrame()
    for pid in product_ids:
        exploded = _explode_bom_tree(bom_master, pid)
        if exploded.empty:
            logger.warning(f"No BOM found for product {pid}")
            continue

        costed = _rollup_costs(exploded, costs)
        costed["finished_product_id"] = pid
        all_boms = all_boms.append(costed, ignore_index=True)

    if not all_boms.empty:
        summary = (
            all_boms.groupby("finished_product_id")
            .agg(total_material_cost=("line_cost", "sum"), component_count=("component_id", "nunique"))
            .reset_index()
        )
        logger.info(f"Resolved BOMs for {len(summary)} products")
    else:
        summary = pd.DataFrame()
        logger.warning("No BOMs resolved — check bom_master data")

    return all_boms
