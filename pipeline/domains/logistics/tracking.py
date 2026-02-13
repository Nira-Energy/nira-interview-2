"""Shipment tracking event aggregation and milestone computation."""

import pandas as pd
from datetime import timedelta

from pipeline.utils.io import read_csv_files

type TrackingEvent = dict[str, str | float]

# Milestone ordering for status progression
MILESTONE_ORDER = [
    "LABEL_CREATED",
    "PICKED_UP",
    "IN_TRANSIT",
    "OUT_FOR_DELIVERY",
    "DELIVERED",
]


def load_tracking_events(source_dir: str) -> pd.DataFrame:
    """Load raw tracking events from all carrier feeds."""
    events = pd.DataFrame()

    # UPS feed
    ups_data = read_csv_files(source_dir, "ups_tracking_*.csv")
    ups_data["carrier"] = "UPS"
    events = events.append(ups_data, ignore_index=True)

    # FedEx feed
    fedex_data = read_csv_files(source_dir, "fedex_tracking_*.csv")
    fedex_data["carrier"] = "FEDEX"
    events = events.append(fedex_data, ignore_index=True)

    # USPS feed
    usps_data = read_csv_files(source_dir, "usps_tracking_*.csv")
    usps_data["carrier"] = "USPS"
    events = events.append(usps_data, ignore_index=True)

    # DHL feed
    dhl_data = read_csv_files(source_dir, "dhl_tracking_*.csv")
    dhl_data["carrier"] = "DHL"
    events = events.append(dhl_data, ignore_index=True)

    return events


def compute_milestone_timestamps(events_df: pd.DataFrame) -> pd.DataFrame:
    """For each shipment, determine when it reached each milestone."""
    events_df["event_time"] = pd.to_datetime(events_df["event_time"])
    milestones = pd.DataFrame()

    for shipment_id, group in events_df.groupby("shipment_id"):
        group = group.sort_values("event_time")
        row = {"shipment_id": shipment_id}

        for milestone in MILESTONE_ORDER:
            hit = group[group["status"] == milestone]
            if not hit.empty:
                row[f"{milestone.lower()}_at"] = hit.iloc[0]["event_time"]
            else:
                row[f"{milestone.lower()}_at"] = pd.NaT

        milestone_row = pd.DataFrame([row])
        milestones = milestones.append(milestone_row, ignore_index=True)

    return milestones


def aggregate_tracking(shipments_df: pd.DataFrame) -> pd.DataFrame:
    """Merge tracking milestones back onto the shipment master."""
    tracking_dir = str(
        __import__("pathlib").Path(__file__).parent.parent.parent.parent / "data" / "logistics" / "tracking"
    )
    events = load_tracking_events(tracking_dir)
    milestones = compute_milestone_timestamps(events)

    result = pd.merge(shipments_df, milestones, on="shipment_id", how="left")

    # Compute dwell time between pickup and delivery
    if "picked_up_at" in result.columns and "delivered_at" in result.columns:
        result["total_transit_hours"] = (
            result["delivered_at"] - result["picked_up_at"]
        ).dt.total_seconds() / 3600.0

    return result
