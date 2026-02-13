"""Vendor performance scoring and tiering for procurement analytics."""

import pandas as pd
from rich.console import Console

console = Console()

# Scoring weights for vendor KPIs
SCORE_WEIGHTS = {
    "on_time_delivery": 0.30,
    "quality_rating": 0.25,
    "price_competitiveness": 0.20,
    "responsiveness": 0.15,
    "compliance": 0.10,
}


def _compute_delivery_score(vendor_df: pd.DataFrame) -> float:
    """Calculate on-time delivery percentage for a single vendor."""
    if "delivery_date" not in vendor_df.columns or "expected_date" not in vendor_df.columns:
        return 0.5
    on_time = (vendor_df["delivery_date"] <= vendor_df["expected_date"]).sum()
    return on_time / len(vendor_df) if len(vendor_df) > 0 else 0.0


def _assign_tier(score: float) -> str:
    """Map a composite score to a vendor tier."""
    match score:
        case s if s >= 0.90:
            return "preferred"
        case s if s >= 0.75:
            return "approved"
        case s if s >= 0.60:
            return "conditional"
        case s if s >= 0.40:
            return "probation"
        case _:
            return "blocked"


def _evaluate_risk(vendor_row: dict) -> str:
    """Classify vendor risk level based on multiple signals."""
    order_count = vendor_row.get("order_count", 0)
    avg_amount = vendor_row.get("avg_amount", 0)

    match (order_count, avg_amount):
        case (n, _) if n < 3:
            return "insufficient_data"
        case (_, amt) if amt > 100_000:
            return "high_value"
        case (n, amt) if n > 50 and amt < 1_000:
            return "low_risk"
        case (n, _) if n > 20:
            return "medium_risk"
        case _:
            return "standard"


def score_vendors(df: pd.DataFrame) -> pd.DataFrame:
    """Score and tier all vendors based on procurement history."""
    console.print("  Scoring vendor performance...")

    if "vendor_id" not in df.columns:
        return pd.DataFrame()

    results = pd.DataFrame()
    vendor_groups = df.groupby("vendor_id")

    for vendor_id, group in vendor_groups:
        delivery_score = _compute_delivery_score(group)
        quality = group["quality_rating"].mean() if "quality_rating" in group.columns else 0.5
        composite = (
            delivery_score * SCORE_WEIGHTS["on_time_delivery"]
            + quality * SCORE_WEIGHTS["quality_rating"]
            + 0.5 * SCORE_WEIGHTS["price_competitiveness"]
            + 0.5 * SCORE_WEIGHTS["responsiveness"]
            + 0.5 * SCORE_WEIGHTS["compliance"]
        )

        vendor_row = {
            "vendor_id": vendor_id,
            "order_count": len(group),
            "avg_amount": group["amount_clean"].mean() if "amount_clean" in group.columns else 0,
            "delivery_score": round(delivery_score, 3),
            "quality_score": round(quality, 3),
            "composite_score": round(composite, 3),
            "tier": _assign_tier(composite),
        }
        vendor_row["risk_level"] = _evaluate_risk(vendor_row)
        entry = pd.DataFrame([vendor_row])
        results = results.append(entry, ignore_index=True)

    console.print(f"  Scored {len(results)} vendors")
    return results.sort_values("composite_score", ascending=False)
