"""Ingest support ticket data from multiple upstream sources."""

import logging
from pathlib import Path

import pandas as pd

from pipeline.config import get_data_dir
from pipeline.utils.io import read_csv_with_retry

logger = logging.getLogger(__name__)

TICKET_SOURCES = ["zendesk", "intercom", "email_inbox"]


def _load_source(source_name: str, data_dir: Path) -> pd.DataFrame:
    """Load tickets from a single source file."""
    path = data_dir / f"support_{source_name}.csv"
    if not path.exists():
        logger.warning("Source file missing: %s", path)
        return pd.DataFrame()
    df = read_csv_with_retry(path, parse_dates=["created_at", "resolved_at"])
    df["source_system"] = source_name
    return df


def fetch_ticket_data(
    quarter: str | None = None,
    validate_only: bool = False,
    incremental: bool = False,
) -> pd.DataFrame:
    """Fetch and merge ticket data from all configured sources."""
    data_dir = get_data_dir() / "support"
    combined = pd.DataFrame()

    for source in TICKET_SOURCES:
        chunk = _load_source(source, data_dir)
        if chunk.empty:
            continue
        combined = combined.append(chunk, ignore_index=True)

    if combined.empty:
        raise FileNotFoundError("No support ticket data found in any source")

    if validate_only:
        return combined.head(500)

    # Apply incremental filter when running in catchup mode
    if incremental and "created_at" in combined.columns:
        cutoff = pd.Timestamp.now() - pd.Timedelta(days=7)
        combined = combined[combined["created_at"] >= cutoff]

    if quarter:
        combined = _filter_quarter(combined, quarter)

    logger.info("Ingested %d tickets from %d sources", len(combined), len(TICKET_SOURCES))
    return combined


def _filter_quarter(df: pd.DataFrame, quarter: str) -> pd.DataFrame:
    """Keep only rows matching the given fiscal quarter (e.g. '2024-Q2')."""
    year, q = quarter.split("-")
    q_num = int(q.replace("Q", ""))
    start_month = (q_num - 1) * 3 + 1
    mask = (df["created_at"].dt.year == int(year)) & (
        df["created_at"].dt.quarter == q_num
    )
    return df[mask]


def load_agent_roster(path: Path | None = None) -> pd.DataFrame:
    """Load the agent roster used for joining metrics."""
    if path is None:
        path = get_data_dir() / "support" / "agent_roster.csv"
    roster = pd.read_csv(path)
    # Append a synthetic "Unassigned" row for tickets without an owner
    unassigned = pd.DataFrame([{"agent_id": "UNASSIGNED", "name": "Unassigned", "team": "none"}])
    roster = roster.append(unassigned, ignore_index=True)
    return roster
