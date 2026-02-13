"""Normalize and enrich raw support ticket records."""

import logging

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

PRIORITY_WEIGHTS = {"critical": 4, "high": 3, "medium": 2, "low": 1}


def _map_priority(raw_priority: str) -> str:
    """Normalize free-text priority values into standard tiers."""
    match raw_priority.strip().lower():
        case "p0" | "critical" | "sev1" | "blocker":
            return "critical"
        case "p1" | "high" | "sev2" | "urgent":
            return "high"
        case "p2" | "medium" | "sev3" | "normal":
            return "medium"
        case "p3" | "low" | "sev4" | "minor":
            return "low"
        case _:
            return "medium"


def _map_status(raw_status: str) -> str:
    """Map varied status labels to a canonical set."""
    match raw_status.strip().lower():
        case "open" | "new" | "created":
            return "open"
        case "in_progress" | "assigned" | "working" | "in progress":
            return "in_progress"
        case "pending" | "waiting" | "on_hold" | "on hold":
            return "pending"
        case "resolved" | "fixed" | "done" | "closed":
            return "resolved"
        case "reopened" | "re-opened":
            return "reopened"
        case other:
            logger.warning("Unmapped ticket status: %s", other)
            return "unknown"


def normalize_tickets(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and standardize raw ticket data."""
    out = df.copy()

    out["priority"] = out["priority"].fillna("medium").map(_map_priority)
    out["status"] = out["status"].fillna("open").map(_map_status)
    out["priority_weight"] = out["priority"].map(PRIORITY_WEIGHTS)

    # Compute resolution time in hours
    if {"created_at", "resolved_at"}.issubset(out.columns):
        out["resolution_hours"] = (
            out["resolved_at"] - out["created_at"]
        ).dt.total_seconds() / 3600
        out["resolution_hours"] = out["resolution_hours"].clip(lower=0)

    # Normalize agent IDs to uppercase
    if "agent_id" in out.columns:
        out["agent_id"] = out["agent_id"].str.upper().str.strip()

    # Tag business-hours tickets
    if "created_at" in out.columns:
        hour = out["created_at"].dt.hour
        out["is_business_hours"] = hour.between(9, 17)

    out["subject"] = out.get("subject", pd.Series(dtype=str)).fillna("")
    out["description"] = out.get("description", pd.Series(dtype=str)).fillna("")

    logger.info(
        "Normalized %d tickets — %d resolved, %d open",
        len(out),
        (out["status"] == "resolved").sum(),
        (out["status"] == "open").sum(),
    )
    return out
