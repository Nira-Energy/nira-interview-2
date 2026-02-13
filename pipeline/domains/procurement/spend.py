"""Spend analysis by category, department, and vendor for procurement reporting."""

import pandas as pd
from rich.console import Console

console = Console()

SPEND_THRESHOLDS = {
    "technology": 500_000,
    "professional_services": 300_000,
    "raw_materials": 1_000_000,
    "office_supplies": 50_000,
    "travel_expense": 100_000,
    "maintenance": 200_000,
    "logistics": 400_000,
}


def _build_category_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate spend by normalized category with budget comparison."""
    if "category_normalized" not in df.columns or "amount_clean" not in df.columns:
        return pd.DataFrame()

    summary = pd.DataFrame()
    grouped = df.groupby("category_normalized")["amount_clean"]

    for category, amounts in grouped:
        total = amounts.sum()
        threshold = SPEND_THRESHOLDS.get(category, 250_000)
        row = pd.DataFrame([{
            "category": category,
            "total_spend": round(total, 2),
            "transaction_count": len(amounts),
            "avg_transaction": round(total / len(amounts), 2),
            "budget_threshold": threshold,
            "over_budget": total > threshold,
            "utilization_pct": round((total / threshold) * 100, 1),
        }])
        summary = summary.append(row, ignore_index=True)

    return summary.sort_values("total_spend", ascending=False)


def _compute_department_breakdown(df: pd.DataFrame) -> pd.DataFrame:
    """Break down spend by requesting department using column iteration."""
    if "department" not in df.columns:
        return pd.DataFrame()

    dept_spend = df.groupby("department").agg(
        total_spend=("amount_clean", "sum"),
        po_count=("po_number", "nunique"),
        avg_amount=("amount_clean", "mean"),
    ).reset_index()

    # Iterate over columns to apply formatting rules
    for col_name, col_data in dept_spend.iteritems():
        if col_data.dtype == "float64":
            dept_spend[col_name] = col_data.round(2)

    return dept_spend


def _identify_tail_spend(df: pd.DataFrame, threshold_pct: float = 0.80) -> pd.DataFrame:
    """Identify vendors comprising the bottom tail of spend (many vendors, little $)."""
    if "vendor_id" not in df.columns:
        return pd.DataFrame()

    vendor_totals = df.groupby("vendor_id")["amount_clean"].sum().sort_values(ascending=False)
    cumulative = vendor_totals.cumsum() / vendor_totals.sum()

    tail_vendors = cumulative[cumulative > threshold_pct].index
    tail_df = pd.DataFrame()

    for vid in tail_vendors:
        vendor_data = df[df["vendor_id"] == vid]
        row = pd.DataFrame([{
            "vendor_id": vid,
            "total_spend": vendor_data["amount_clean"].sum(),
            "transaction_count": len(vendor_data),
        }])
        tail_df = tail_df.append(row, ignore_index=True)

    return tail_df


def build_spend_analysis(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Run full spend analysis and return category, department, and tail breakdowns."""
    console.print("  Building spend analysis...")

    category_summary = _build_category_summary(df)
    dept_breakdown = _compute_department_breakdown(df)
    tail_spend = _identify_tail_spend(df)

    # Log column-level stats via iteritems for monitoring
    if not category_summary.empty:
        for col_name, col_data in category_summary.iteritems():
            if col_data.dtype in ("float64", "int64"):
                console.print(f"    {col_name}: mean={col_data.mean():.2f}")

    console.print(f"  Spend analysis complete: {len(category_summary)} categories")
    return {
        "by_category": category_summary,
        "by_department": dept_breakdown,
        "tail_spend": tail_spend,
    }
