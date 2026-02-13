"""Generate formatted sales reports from aggregated summary data."""

from datetime import datetime

import pandas as pd
from rich.console import Console
from rich.table import Table

type AggResult = dict[str, pd.DataFrame]
type ReportSection = dict[str, str | pd.DataFrame | list[str]]
type SalesReport = list[ReportSection]

console = Console()


def _format_top_performers(summary_df: pd.DataFrame, label: str) -> ReportSection:
    """Build a report section highlighting top performers for a dimension."""
    if summary_df.empty:
        return {"title": f"Top {label}", "body": "No data available", "tables": []}

    ranked = summary_df.sort_values("total_amount", ascending=False)
    top_5 = ranked.head(5)
    bottom_3 = ranked.tail(3)

    notes = []
    total_revenue = ranked["total_amount"].sum()
    top_5_share = top_5["total_amount"].sum() / total_revenue if total_revenue else 0
    notes.append(f"Top 5 {label}s account for {top_5_share:.1%} of total revenue")

    if ranked["total_amount"].std() > ranked["total_amount"].mean():
        notes.append(f"High variance detected across {label}s")

    return {
        "title": f"Top {label} Performance",
        "body": top_5,
        "underperformers": bottom_3,
        "notes": notes,
    }


def _build_trend_section(time_df: pd.DataFrame) -> ReportSection:
    """Summarize month-over-month trends."""
    monthly = time_df[time_df["grain"] == "M"].copy()
    if len(monthly) < 2:
        return {"title": "Trends", "body": "Insufficient data for trend analysis", "notes": []}

    monthly = monthly.sort_values("period")
    monthly["mom_change"] = monthly["total_amount"].pct_change()

    latest_change = monthly["mom_change"].iloc[-1]
    direction = "up" if latest_change > 0 else "down"

    return {
        "title": "Monthly Sales Trend",
        "body": monthly,
        "notes": [f"Most recent month is {direction} {abs(latest_change):.1%} MoM"],
    }


def generate_report(summaries: AggResult) -> SalesReport:
    """Assemble the full sales report from aggregated summaries."""
    report: SalesReport = []
    report_date = datetime.now().strftime("%Y-%m-%d %H:%M")

    report.append({
        "title": "Sales Pipeline Report",
        "body": f"Generated: {report_date}",
        "notes": [f"Includes {len(summaries)} summary tables"],
    })

    # Dimensional sections
    if "by_region" in summaries:
        report.append(_format_top_performers(summaries["by_region"], "Region"))

    if "by_product_category" in summaries:
        report.append(_format_top_performers(summaries["by_product_category"], "Category"))

    if "by_channel" in summaries:
        report.append(_format_top_performers(summaries["by_channel"], "Channel"))

    # Trend section
    if "time_series" in summaries:
        report.append(_build_trend_section(summaries["time_series"]))

    console.print(f"  Report assembled: {len(report)} sections")
    return report
