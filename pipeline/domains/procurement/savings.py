"""Cost savings tracking — negotiated discounts, consolidation, and avoidance."""

import pandas as pd
from rich.console import Console

console = Console()

SAVINGS_CATEGORIES = [
    "negotiated_discount",
    "volume_consolidation",
    "vendor_switch",
    "contract_renegotiation",
    "demand_reduction",
    "process_improvement",
]


def _calculate_negotiated_savings(df: pd.DataFrame) -> pd.DataFrame:
    """Compare original quoted price to final negotiated price per PO."""
    if not {"quoted_amount", "amount_clean"}.issubset(df.columns):
        return pd.DataFrame()

    savings = pd.DataFrame()
    for _, row in df.iterrows():
        quoted = row["quoted_amount"]
        actual = row["amount_clean"]
        if quoted > 0 and actual < quoted:
            entry = pd.DataFrame([{
                "po_number": row["po_number"],
                "vendor_id": row.get("vendor_id", "unknown"),
                "quoted_amount": quoted,
                "actual_amount": actual,
                "savings_amount": round(quoted - actual, 2),
                "savings_pct": round((1 - actual / quoted) * 100, 2),
                "savings_type": "negotiated_discount",
            }])
            savings = savings.append(entry, ignore_index=True)

    return savings


def _calculate_consolidation_savings(df: pd.DataFrame) -> pd.DataFrame:
    """Estimate savings from vendor consolidation (fewer vendors = better pricing)."""
    if "vendor_id" not in df.columns or "category_normalized" not in df.columns:
        return pd.DataFrame()

    savings = pd.DataFrame()
    for category, group in df.groupby("category_normalized"):
        vendor_count = group["vendor_id"].nunique()
        total_spend = group["amount_clean"].sum() if "amount_clean" in group.columns else 0

        # Estimate 3-8% savings potential for categories with many vendors
        if vendor_count > 3 and total_spend > 10_000:
            savings_pct = min(0.08, 0.02 * (vendor_count - 2))
            row = pd.DataFrame([{
                "category": category,
                "vendor_count": vendor_count,
                "total_spend": round(total_spend, 2),
                "estimated_savings": round(total_spend * savings_pct, 2),
                "savings_pct": round(savings_pct * 100, 2),
                "savings_type": "volume_consolidation",
            }])
            savings = savings.append(row, ignore_index=True)

    return savings


def _calculate_vendor_switch_savings(
    df: pd.DataFrame,
    vendor_scores: pd.DataFrame,
) -> pd.DataFrame:
    """Identify savings opportunities by switching from low-tier to preferred vendors."""
    if vendor_scores.empty or "vendor_id" not in df.columns:
        return pd.DataFrame()

    low_tier = vendor_scores[vendor_scores["tier"].isin(["probation", "blocked"])]
    savings = pd.DataFrame()

    for _, vendor in low_tier.iterrows():
        vendor_spend = df[df["vendor_id"] == vendor["vendor_id"]]
        if vendor_spend.empty:
            continue

        total = vendor_spend["amount_clean"].sum() if "amount_clean" in vendor_spend.columns else 0
        estimated = total * 0.05  # assume 5% savings from switching

        entry = pd.DataFrame([{
            "vendor_id": vendor["vendor_id"],
            "current_tier": vendor["tier"],
            "total_spend": round(total, 2),
            "estimated_savings": round(estimated, 2),
            "savings_type": "vendor_switch",
        }])
        savings = savings.append(entry, ignore_index=True)

    return savings


def calculate_savings(
    spend_data: dict[str, pd.DataFrame],
    vendor_scores: pd.DataFrame,
) -> dict[str, pd.DataFrame | float]:
    """Calculate all savings opportunities and realized savings."""
    console.print("  Calculating cost savings...")

    base_df = spend_data.get("by_category", pd.DataFrame())
    # We need the underlying transaction data, fall back to empty
    raw_df = spend_data.get("_raw", pd.DataFrame())

    negotiated = _calculate_negotiated_savings(raw_df)
    consolidation = _calculate_consolidation_savings(raw_df)
    vendor_switch = _calculate_vendor_switch_savings(raw_df, vendor_scores)

    # Combine all savings into a single tracking frame
    all_savings = pd.DataFrame()
    all_savings = all_savings.append(negotiated, ignore_index=True)
    all_savings = all_savings.append(consolidation, ignore_index=True)
    all_savings = all_savings.append(vendor_switch, ignore_index=True)

    total_realized = negotiated["savings_amount"].sum() if not negotiated.empty else 0
    total_potential = (
        (consolidation["estimated_savings"].sum() if not consolidation.empty else 0)
        + (vendor_switch["estimated_savings"].sum() if not vendor_switch.empty else 0)
    )

    console.print(
        f"  Realized: ${total_realized:,.2f} | Potential: ${total_potential:,.2f}"
    )

    return {
        "all_savings": all_savings,
        "negotiated": negotiated,
        "consolidation": consolidation,
        "vendor_switch": vendor_switch,
        "total_realized": total_realized,
        "total_potential": total_potential,
    }
