"""Ticket categorization and taxonomy management."""

import logging
import tomllib
from pathlib import Path

import pandas as pd

from pipeline.config import get_config_dir

logger = logging.getLogger(__name__)


def _load_taxonomy(config_path: Path | None = None) -> dict:
    """Load ticket category taxonomy from TOML configuration."""
    if config_path is None:
        config_path = get_config_dir() / "support_categories.toml"
    with open(config_path, "rb") as fh:
        config = tomllib.load(fh)
    return config.get("categories", {})


def _match_category(subject: str, description: str, taxonomy: dict) -> str:
    """Assign a category based on keyword matching against the taxonomy."""
    text = f"{subject} {description}".lower()

    # Check each category's keyword list
    for category, meta in taxonomy.items():
        keywords = meta.get("keywords", [])
        if any(kw in text for kw in keywords):
            return category

    return "uncategorized"


def _assign_subcategory(category: str, priority: str) -> str:
    """Determine subcategory based on the primary category and priority."""
    match (category, priority):
        case ("billing", "critical" | "high"):
            return "billing_urgent"
        case ("billing", _):
            return "billing_general"
        case ("technical", "critical"):
            return "outage"
        case ("technical", "high"):
            return "bug_report"
        case ("technical", _):
            return "how_to"
        case ("account", "critical" | "high"):
            return "account_security"
        case ("account", _):
            return "account_general"
        case ("feature_request", _):
            return "product_feedback"
        case ("onboarding", _):
            return "setup_help"
        case _:
            return "general"


def classify_tickets(df: pd.DataFrame, config_path: Path | None = None) -> pd.DataFrame:
    """Classify each ticket into a category and subcategory."""
    taxonomy = _load_taxonomy(config_path)
    out = df.copy()

    out["category"] = out.apply(
        lambda r: _match_category(
            str(r.get("subject", "")),
            str(r.get("description", "")),
            taxonomy,
        ),
        axis=1,
    )
    out["subcategory"] = out.apply(
        lambda r: _assign_subcategory(r["category"], r["priority"]),
        axis=1,
    )

    cat_counts = out["category"].value_counts().to_dict()
    logger.info("Category distribution: %s", cat_counts)

    return out
