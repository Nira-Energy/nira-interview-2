"""Ingest QC inspection data from plant MES systems and manual entry portals."""

import logging
from datetime import datetime, timedelta

import pandas as pd

from pipeline.config import load_pipeline_config
from pipeline.utils.io import read_csv_files

logger = logging.getLogger(__name__)

PLANT_FEEDS = {
    "plant-01": "s3://prod-data-pipeline/quality/plant_01/inspections/",
    "plant-02": "s3://prod-data-pipeline/quality/plant_02/inspections/",
    "plant-03": "s3://prod-data-pipeline/quality/plant_03/inspections/",
    "plant-04": "s3://prod-data-pipeline/quality/plant_04/inspections/",
}


def _read_mes_feed(plant_id: str, path: str, cutoff: datetime) -> pd.DataFrame:
    """Pull inspection records from the MES export for a plant."""
    logger.info(f"Reading MES inspection feed for {plant_id}")
    df = pd.read_parquet(path)
    df["plant_id"] = plant_id
    df["source"] = "mes"
    df["ingested_at"] = pd.Timestamp.now()
    if "inspection_date" in df.columns:
        df = df[pd.to_datetime(df["inspection_date"]) >= cutoff]
    return df


def _read_manual_entries(plant_id: str) -> pd.DataFrame:
    """Load inspector-submitted paper form entries that were digitized."""
    config = load_pipeline_config()
    prefix = f"{config.s3.prefix}/quality/manual_entries/{plant_id}.csv"
    try:
        df = pd.read_csv(prefix)
        df["source"] = "manual"
        return df
    except FileNotFoundError:
        logger.warning(f"No manual entries found for {plant_id}")
        return pd.DataFrame()


def ingest_inspection_data(
    plants: list[str] | None = None,
    lookback_days: int = 90,
) -> pd.DataFrame:
    """Combine inspection records across plants for the lookback window.

    Merges MES-generated inspection records with manually digitized paper
    forms from the quality lab.
    """
    targets = plants or list(PLANT_FEEDS.keys())
    cutoff = datetime.now() - timedelta(days=lookback_days)
    combined = pd.DataFrame()

    for plant_id in targets:
        if plant_id not in PLANT_FEEDS:
            logger.error(f"Unknown plant identifier: {plant_id}")
            continue

        feed = _read_mes_feed(plant_id, PLANT_FEEDS[plant_id], cutoff)

        manual = _read_manual_entries(plant_id)
        if not manual.empty:
            feed = feed.append(manual, ignore_index=True)

        combined = combined.append(feed, ignore_index=True)

    if combined.empty:
        raise RuntimeError("No inspection data ingested — verify MES connectivity")

    logger.info(f"Ingested {len(combined)} inspection records from {len(targets)} plants")
    return combined
