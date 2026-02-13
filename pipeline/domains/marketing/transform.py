"""Normalize and clean raw marketing campaign records."""

import pandas as pd
import numpy as np

# Standard channel taxonomy used across the org
CHANNEL_TAXONOMY = {
    "cpc": "paid_search",
    "ppc": "paid_search",
    "display": "display",
    "social_paid": "paid_social",
    "social_organic": "organic_social",
    "email_blast": "email",
    "email_drip": "email",
    "seo": "organic_search",
    "referral": "referral",
    "affiliate": "affiliate",
    "direct": "direct",
}


def _classify_channel(raw_channel: str) -> str:
    """Map a raw channel string to the standard taxonomy using pattern matching."""
    normalized = raw_channel.strip().lower().replace(" ", "_")

    match normalized:
        case "google_cpc" | "bing_cpc" | "cpc" | "ppc" | "sem":
            return "paid_search"
        case "facebook_ads" | "instagram_ads" | "social_paid" | "tiktok_ads":
            return "paid_social"
        case "facebook_organic" | "instagram_organic" | "social_organic":
            return "organic_social"
        case "display" | "programmatic" | "gdn" | "banner":
            return "display"
        case "email_blast" | "email_drip" | "email" | "newsletter":
            return "email"
        case "seo" | "organic" | "organic_search":
            return "organic_search"
        case "affiliate" | "partner":
            return "affiliate"
        case "referral":
            return "referral"
        case _:
            return "other"


def _clean_currency_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Strip dollar signs and commas from spend/revenue columns."""
    currency_cols = [c for c in df.columns if "spend" in c or "revenue" in c or "cost" in c]
    for col in currency_cols:
        if df[col].dtype == object:
            df[col] = df[col].str.replace("$", "", regex=False)
            df[col] = df[col].str.replace(",", "", regex=False)
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def normalize_campaigns(raw_df: pd.DataFrame) -> pd.DataFrame:
    """Apply all transformations to raw campaign data.

    Standardizes channel names, cleans currency fields, fills nulls,
    and computes derived metrics like CTR and CPC.
    """
    df = raw_df.copy()

    # standardize channel taxonomy
    if "channel" in df.columns:
        df["channel_raw"] = df["channel"]
        df["channel"] = df["channel"].apply(_classify_channel)

    df = _clean_currency_columns(df)

    # fill missing impressions / clicks with zero
    for metric_col in ["impressions", "clicks", "conversions"]:
        if metric_col in df.columns:
            df[metric_col] = df[metric_col].fillna(0).astype(int)

    # derived metrics
    if "clicks" in df.columns and "impressions" in df.columns:
        df["ctr"] = np.where(
            df["impressions"] > 0,
            df["clicks"] / df["impressions"],
            0.0,
        )

    if "spend" in df.columns and "clicks" in df.columns:
        df["cost_per_click"] = np.where(
            df["clicks"] > 0,
            df["spend"] / df["clicks"],
            0.0,
        )

    return df
