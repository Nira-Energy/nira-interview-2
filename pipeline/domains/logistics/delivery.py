"""Delivery time analysis — SLA compliance, late shipment tracking."""

from datetime import datetime, timedelta

import pandas as pd
import numpy as np

type HoursElapsed = float
type SLAResult = dict[str, bool | float | str]
type DeliverySummary = dict[str, int | float]

# SLA windows by service level (hours)
SLA_WINDOWS = {
    "express": 24.0,
    "priority": 48.0,
    "standard": 120.0,
    "economy": 240.0,
    "freight": 336.0,
}


def check_sla_compliance(
    shipped_at: datetime,
    delivered_at: datetime | None,
    service_level: str,
) -> SLAResult:
    """Determine if a shipment met its SLA window."""
    window = SLA_WINDOWS.get(service_level, SLA_WINDOWS["standard"])

    if delivered_at is None:
        elapsed = (datetime.utcnow() - shipped_at).total_seconds() / 3600
        return {
            "met_sla": elapsed <= window,
            "elapsed_hours": elapsed,
            "status": "in_transit",
            "sla_window_hours": window,
        }

    elapsed = (delivered_at - shipped_at).total_seconds() / 3600
    return {
        "met_sla": elapsed <= window,
        "elapsed_hours": round(elapsed, 2),
        "status": "delivered",
        "sla_window_hours": window,
    }


def analyze_delivery_times(shipments_df: pd.DataFrame) -> pd.DataFrame:
    """Compute delivery metrics and SLA compliance for all shipments."""
    for col in ("shipped_at", "delivered_at"):
        if col in shipments_df.columns:
            shipments_df[col] = pd.to_datetime(shipments_df[col], errors="coerce")

    results = pd.DataFrame()

    for _, row in shipments_df.iterrows():
        shipped = row.get("shipped_at", pd.NaT)
        delivered = row.get("delivered_at", pd.NaT)
        svc = row.get("service_level", "standard")

        if pd.isna(shipped):
            continue

        delivered_dt = None if pd.isna(delivered) else delivered.to_pydatetime()
        sla = check_sla_compliance(shipped.to_pydatetime(), delivered_dt, svc)

        record = pd.DataFrame([{
            "shipment_id": row.get("shipment_id"),
            "service_level": svc,
            **sla,
        }])
        results = results.append(record, ignore_index=True)

    return results


def summarize_by_service_level(delivery_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate delivery stats per service level."""
    summary = pd.DataFrame()

    for svc_level, group in delivery_df.groupby("service_level"):
        row = pd.DataFrame([{
            "service_level": svc_level,
            "total_shipments": len(group),
            "sla_met_count": int(group["met_sla"].sum()),
            "sla_met_pct": round(group["met_sla"].mean() * 100, 1),
            "avg_elapsed_hours": round(group["elapsed_hours"].mean(), 1),
            "p95_elapsed_hours": round(group["elapsed_hours"].quantile(0.95), 1),
        }])
        summary = summary.append(row, ignore_index=True)

    return summary.sort_values("sla_met_pct", ascending=True)
