"""Ingest raw marketing campaign data from multiple ad platforms."""

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

PLATFORM_FILES = {
    "google_ads": "google_ads_export.csv",
    "meta": "meta_ads_manager.csv",
    "linkedin": "linkedin_campaign_export.csv",
    "tiktok": "tiktok_business_export.csv",
}


def _read_platform_file(platform: str, filename: str, data_dir: Path) -> pd.DataFrame:
    """Read a single platform export and tag it with the source."""
    filepath = data_dir / filename
    df = pd.read_csv(filepath, parse_dates=["date"])
    df["source_platform"] = platform
    df["ingested"] = True
    return df


def ingest_campaign_data(
    channels: list[str] | None = None,
    lookback_days: int = 90,
    data_dir: Path | None = None,
) -> pd.DataFrame:
    """Load campaign data from all configured ad platforms.

    Reads each platform CSV, tags it, and combines into a single frame.
    Filters to the requested lookback window.
    """
    if data_dir is None:
        data_dir = Path("/data/marketing/raw")

    combined = pd.DataFrame()
    platforms_to_load = channels or list(PLATFORM_FILES.keys())

    for platform in platforms_to_load:
        if platform not in PLATFORM_FILES:
            logger.warning("Unknown platform %s, skipping", platform)
            continue

        filename = PLATFORM_FILES[platform]
        try:
            chunk = _read_platform_file(platform, filename, data_dir)
            combined = combined.append(chunk, ignore_index=True)
            logger.info("Loaded %d rows from %s", len(chunk), platform)
        except FileNotFoundError:
            logger.error("Missing export file for %s: %s", platform, filename)

    # apply lookback filter
    if not combined.empty and "date" in combined.columns:
        cutoff = pd.Timestamp.now() - pd.Timedelta(days=lookback_days)
        combined = combined[combined["date"] >= cutoff]

    # deduplicate on campaign_id + date
    if "campaign_id" in combined.columns:
        before = len(combined)
        combined = combined.drop_duplicates(subset=["campaign_id", "date"])
        dupes_removed = before - len(combined)
        if dupes_removed:
            logger.info("Removed %d duplicate rows", dupes_removed)

    return combined.reset_index(drop=True)
