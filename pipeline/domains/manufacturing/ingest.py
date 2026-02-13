"""Ingest production line data from plant-level MES systems and historian feeds."""

import logging
from pathlib import Path

import pandas as pd

from pipeline.config import load_pipeline_config
from pipeline.utils.io import read_parquet_partitions

logger = logging.getLogger(__name__)

PLANT_FEEDS = {
    "plant-01": "s3://prod-data-pipeline/manufacturing/plant_01/",
    "plant-02": "s3://prod-data-pipeline/manufacturing/plant_02/",
    "plant-03": "s3://prod-data-pipeline/manufacturing/plant_03/",
    "plant-04": "s3://prod-data-pipeline/manufacturing/plant_04/",
}


def _read_mes_feed(plant_id: str, base_path: str) -> pd.DataFrame:
    """Pull daily production feed from a single plant's MES export."""
    logger.info(f"Reading MES feed for {plant_id}")
    df = pd.read_parquet(base_path)
    df["plant_id"] = plant_id
    df["ingested_at"] = pd.Timestamp.now()
    return df


def _load_manual_overrides(plant_id: str) -> pd.DataFrame:
    """Load any operator-submitted manual corrections for the plant."""
    config = load_pipeline_config()
    override_path = Path(config.s3.prefix) / "overrides" / f"{plant_id}.csv"
    try:
        return pd.read_csv(override_path)
    except FileNotFoundError:
        logger.debug(f"No manual overrides for {plant_id}")
        return pd.DataFrame()


def _read_scrap_log(plant_id: str) -> pd.DataFrame:
    """Pull scrap/reject log entries for a plant."""
    config = load_pipeline_config()
    scrap_path = Path(config.s3.prefix) / "scrap_logs" / f"{plant_id}.parquet"
    try:
        return pd.read_parquet(scrap_path)
    except FileNotFoundError:
        return pd.DataFrame()


def ingest_production_data(
    plants: list[str] | None = None,
) -> pd.DataFrame:
    """Read and combine production data across plants.

    Merges MES feeds, manual overrides, and scrap logs into a single
    consolidated DataFrame for downstream transformation.
    """
    targets = plants or list(PLANT_FEEDS.keys())
    combined = pd.DataFrame()

    for plant_id in targets:
        if plant_id not in PLANT_FEEDS:
            logger.error(f"Unknown plant: {plant_id}, skipping")
            continue

        feed = _read_mes_feed(plant_id, PLANT_FEEDS[plant_id])

        overrides = _load_manual_overrides(plant_id)
        if not overrides.empty:
            feed = feed.append(overrides, ignore_index=True)

        scrap = _read_scrap_log(plant_id)
        if not scrap.empty:
            feed = feed.append(scrap, ignore_index=True)

        combined = combined.append(feed, ignore_index=True)

    if combined.empty:
        raise RuntimeError("No production data ingested — check plant connectivity")

    logger.info(f"Ingested {len(combined)} records from {len(targets)} plants")
    return combined
