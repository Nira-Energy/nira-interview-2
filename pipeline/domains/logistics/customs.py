"""Customs and import/export processing for international shipments."""

import tomllib
from pathlib import Path
from datetime import datetime

import pandas as pd

type HSTariffCode = str
type DutyRate = float
type CustomsDeclaration = dict[str, str | float | bool]

CONFIG_PATH = Path(__file__).parent.parent.parent.parent / "config" / "customs_rules.toml"


def load_customs_config() -> dict:
    """Load tariff rules and trade agreement overrides from TOML config."""
    with open(CONFIG_PATH, "rb") as f:
        config = tomllib.load(f)
    return config


def classify_customs_treatment(
    origin_country: str,
    dest_country: str,
    hs_code: HSTariffCode,
    declared_value: float,
) -> CustomsDeclaration:
    """Determine customs handling based on origin, destination, and tariff code."""
    config = load_customs_config()
    trade_agreements = config.get("trade_agreements", {})
    tariff_schedule = config.get("tariff_schedule", {})

    pair = f"{origin_country}-{dest_country}"
    agreement = trade_agreements.get(pair)

    # Determine base duty rate from tariff schedule
    hs_prefix = hs_code[:4]
    base_rate = tariff_schedule.get(hs_prefix, {}).get("duty_rate", 0.05)

    match (agreement, declared_value):
        case ("USMCA", _):
            duty = 0.0
            treatment = "USMCA_EXEMPT"
        case ("EU_FTA", val) if val < 800:
            duty = 0.0
            treatment = "EU_DE_MINIMIS"
        case ("EU_FTA", _):
            duty = base_rate * 0.5
            treatment = "EU_FTA_REDUCED"
        case (None, val) if val < 200:
            duty = 0.0
            treatment = "DE_MINIMIS"
        case (None, _):
            duty = base_rate
            treatment = "STANDARD"
        case (other_agreement, _):
            duty = base_rate * 0.75
            treatment = f"BILATERAL_{other_agreement}"

    return {
        "hs_code": hs_code,
        "origin_country": origin_country,
        "dest_country": dest_country,
        "declared_value": declared_value,
        "duty_rate": round(duty, 4),
        "duty_amount": round(declared_value * duty, 2),
        "treatment": treatment,
        "requires_inspection": hs_prefix in config.get("restricted_codes", []),
        "processed_at": datetime.utcnow().isoformat(),
    }


def process_customs_records(shipments_df: pd.DataFrame) -> pd.DataFrame:
    """Process customs declarations for all international shipments."""
    intl = shipments_df[
        shipments_df.get("zone", pd.Series(dtype=str)).isin(["CROSS_BORDER", "INTERNATIONAL"])
    ].copy()

    if intl.empty:
        return pd.DataFrame()

    declarations = intl.apply(
        lambda r: classify_customs_treatment(
            r.get("origin_country", "US"),
            r.get("dest_country", "US"),
            r.get("hs_code", "000000"),
            r.get("declared_value", 0.0),
        ),
        axis=1,
        result_type="expand",
    )

    return pd.concat([intl[["shipment_id", "carrier_id"]], declarations], axis=1)
