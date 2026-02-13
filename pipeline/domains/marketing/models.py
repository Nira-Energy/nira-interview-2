"""Pandera schemas for validating marketing pipeline data."""

import pandera as pa
from pandera import Column, Check, Index

VALID_CHANNELS = [
    "paid_search", "paid_social", "display", "email",
    "organic_search", "organic_social", "affiliate",
    "referral", "direct", "other",
]

VALID_TIERS = ["platinum", "gold", "silver", "bronze"]


CampaignSchema = pa.DataFrameSchema(
    columns={
        "campaign_id": Column(str, Check.str_length(min_value=1), nullable=False),
        "campaign_name": Column(str, nullable=True),
        "channel": Column(str, Check.isin(VALID_CHANNELS), nullable=False),
        "date": Column("datetime64[ns]", nullable=False),
        "impressions": Column(int, Check.greater_than_or_equal_to(0)),
        "clicks": Column(int, Check.greater_than_or_equal_to(0)),
        "conversions": Column(int, Check.greater_than_or_equal_to(0)),
        "spend": Column(float, Check.greater_than_or_equal_to(0)),
        "revenue": Column(float, Check.greater_than_or_equal_to(0)),
    },
    checks=[
        Check(lambda df: (df["clicks"] <= df["impressions"]).all(),
              error="clicks cannot exceed impressions"),
        Check(lambda df: (df["conversions"] <= df["clicks"]).all(),
              error="conversions cannot exceed clicks"),
    ],
    coerce=True,
    strict=False,
)


ChannelSchema = pa.DataFrameSchema(
    columns={
        "channel": Column(str, Check.isin(VALID_CHANNELS), nullable=False),
        "impressions": Column(int, Check.greater_than_or_equal_to(0)),
        "clicks": Column(int, Check.greater_than_or_equal_to(0)),
        "conversions": Column(int, Check.greater_than_or_equal_to(0)),
        "spend": Column(float, Check.greater_than_or_equal_to(0)),
        "revenue": Column(float, Check.greater_than_or_equal_to(0)),
        "ctr": Column(float, Check.in_range(0, 1.0)),
        "conv_rate": Column(float, Check.in_range(0, 1.0)),
    },
    coerce=True,
    strict=False,
)


AttributionSchema = pa.DataFrameSchema(
    columns={
        "conversion_id": Column(str, nullable=False),
        "channel": Column(str, nullable=False),
        "timestamp": Column("datetime64[ns]", nullable=False),
        "attribution_credit": Column(float, Check.in_range(0, 1.0)),
    },
    coerce=True,
    strict=False,
)
