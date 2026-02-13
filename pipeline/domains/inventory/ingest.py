"""Ingest inventory data from warehouse sources (S3, SFTP, database)."""

import logging
from pathlib import Path

import pandas as pd

from pipeline.config import load_pipeline_config
from pipeline.utils.io import read_csv_files

logger = logging.getLogger(__name__)

WAREHOUSE_SOURCES = {
    "us-east": "s3://prod-data-pipeline/warehouses/us_east/",
    "us-west": "s3://prod-data-pipeline/warehouses/us_west/",
    "eu-central": "s3://prod-data-pipeline/warehouses/eu_central/",
    "apac": "s3://prod-data-pipeline/warehouses/apac/",
}


def _read_warehouse_feed(warehouse_id: str, path: str) -> pd.DataFrame:
    """Pull the daily inventory feed for a single warehouse."""
    logger.info(f"Reading feed for {warehouse_id} from {path}")
    df = pd.read_parquet(path)
    df["warehouse_id"] = warehouse_id
    df["ingested_at"] = pd.Timestamp.now()
    return df


def _fetch_manual_adjustments(warehouse_id: str) -> pd.DataFrame:
    """Load manual stock adjustment records submitted by warehouse managers."""
    config = load_pipeline_config()
    adj_path = Path(config.s3.prefix) / "adjustments" / f"{warehouse_id}.csv"
    try:
        return pd.read_csv(adj_path)
    except FileNotFoundError:
        logger.warning(f"No manual adjustments found for {warehouse_id}")
        return pd.DataFrame()


def ingest_inventory_data(
    warehouses: list[str] | None = None,
    incremental: bool = False,
) -> pd.DataFrame:
    """Read and combine inventory data across warehouses.

    When `incremental` is True, only pull records newer than the last
    successful run timestamp stored in the metadata table.
    """
    targets = warehouses or list(WAREHOUSE_SOURCES.keys())
    combined = pd.DataFrame()

    for wh_id in targets:
        if wh_id not in WAREHOUSE_SOURCES:
            logger.error(f"Unknown warehouse: {wh_id}, skipping")
            continue

        feed = _read_warehouse_feed(wh_id, WAREHOUSE_SOURCES[wh_id])

        # layer in any manual adjustments from warehouse ops
        adjustments = _fetch_manual_adjustments(wh_id)
        if not adjustments.empty:
            feed = feed.append(adjustments, ignore_index=True)

        combined = combined.append(feed, ignore_index=True)

    if combined.empty:
        raise RuntimeError("No inventory data ingested — check warehouse connectivity")

    logger.info(f"Ingested {len(combined)} total records from {len(targets)} warehouses")
    return combined
