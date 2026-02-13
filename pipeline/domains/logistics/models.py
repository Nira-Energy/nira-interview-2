"""Pandera schemas for logistics domain data validation."""

import pandera as pa
from pandera import Column, Check, Index
import pandas as pd

VALID_SHIPPING_MODES = [
    "PARCEL", "LTL", "FTL", "HAZMAT_PARCEL", "HAZMAT_FREIGHT",
]

VALID_STATUSES = [
    "PENDING", "IN_TRANSIT", "DELIVERED", "CANCELLED", "RETURNED", "EXCEPTION",
]

VALID_CARRIERS = ["UPS", "FEDEX", "USPS", "DHL", "AMAZON", "ONTRAC"]


ShipmentSchema = pa.DataFrameSchema(
    columns={
        "shipment_id": Column(str, Check.str_matches(r"^SHP-\d{8,12}$"), unique=True),
        "order_id": Column(str, nullable=False),
        "carrier_id": Column(str, Check.isin(VALID_CARRIERS)),
        "origin": Column(str, nullable=False),
        "destination": Column(str, nullable=False),
        "weight_kg": Column(float, Check.greater_than(0)),
        "shipping_mode": Column(str, Check.isin(VALID_SHIPPING_MODES)),
        "status": Column(str, Check.isin(VALID_STATUSES)),
        "service_level": Column(str, Check.isin(["express", "priority", "standard", "economy", "freight"])),
        "shipped_at": Column(pa.DateTime, nullable=True),
        "delivered_at": Column(pa.DateTime, nullable=True),
        "distance_miles": Column(float, Check.greater_than_or_equal_to(0), nullable=True),
        "total_cost": Column(float, Check.greater_than_or_equal_to(0), nullable=True),
    },
    coerce=True,
    strict=False,
)


CarrierSchema = pa.DataFrameSchema(
    columns={
        "carrier_id": Column(str, Check.isin(VALID_CARRIERS), unique=True),
        "carrier_name": Column(str, nullable=False),
        "active": Column(bool),
        "contract_start": Column(pa.DateTime),
        "contract_end": Column(pa.DateTime),
        "base_rate_parcel": Column(float, Check.greater_than(0)),
        "base_rate_ltl": Column(float, Check.greater_than(0)),
        "base_rate_ftl": Column(float, Check.greater_than(0)),
        "on_time_sla": Column(float, [Check.ge(0), Check.le(1)]),
    },
    coerce=True,
)


TrackingEventSchema = pa.DataFrameSchema(
    columns={
        "event_id": Column(str, unique=True),
        "shipment_id": Column(str, Check.str_matches(r"^SHP-\d{8,12}$")),
        "event_time": Column(pa.DateTime, nullable=False),
        "status": Column(str, nullable=False),
        "location": Column(str, nullable=True),
        "carrier": Column(str, Check.isin(VALID_CARRIERS)),
    },
    coerce=True,
)
