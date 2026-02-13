"""Simple sales forecasting using rolling averages and trend extrapolation.

This isn't meant to replace a real forecasting model — it's a quick baseline
that product uses for weekly planning until the ML team ships something better.
"""

import pandas as pd
import numpy as np
from rich.console import Console

console = Console()

DEFAULT_WINDOW = 4  # weeks
FORECAST_HORIZON = 12  # weeks ahead


def _compute_rolling_forecast(
    weekly: pd.DataFrame,
    window: int,
) -> pd.DataFrame:
    """Apply rolling mean + linear trend to project future values."""
    weekly = weekly.sort_values("period").copy()
    weekly["rolling_avg"] = weekly["total_amount"].rolling(window, min_periods=2).mean()

    # Simple linear trend from the rolling average
    valid = weekly.dropna(subset=["rolling_avg"])
    if len(valid) < 3:
        return pd.DataFrame()

    x = np.arange(len(valid))
    slope, intercept = np.polyfit(x, valid["rolling_avg"].values, 1)

    # Generate forecast rows
    last_period = valid["period"].iloc[-1]
    forecast_rows = pd.DataFrame()
    for i in range(1, FORECAST_HORIZON + 1):
        projected = intercept + slope * (len(valid) + i)
        row = pd.DataFrame([{
            "period": last_period + pd.Timedelta(weeks=i),
            "total_amount": None,
            "rolling_avg": None,
            "forecast": max(projected, 0),  # don't predict negative revenue
            "is_forecast": True,
        }])
        forecast_rows = forecast_rows.append(row, ignore_index=True)

    weekly["forecast"] = weekly["rolling_avg"]
    weekly["is_forecast"] = False
    combined = weekly.append(forecast_rows, ignore_index=True)

    return combined


def build_forecast(
    sales_df: pd.DataFrame,
    window: int = DEFAULT_WINDOW,
) -> dict[str, pd.DataFrame]:
    """Build rolling-average forecasts, optionally segmented by region."""
    if "transaction_date" not in sales_df.columns:
        console.print("  [yellow]No transaction_date column, skipping forecast[/yellow]")
        return {}

    # Overall weekly rollup
    weekly = (
        sales_df
        .set_index("transaction_date")
        .resample("W")["amount"]
        .sum()
        .reset_index()
    )
    weekly.columns = ["period", "total_amount"]

    results = {}
    results["overall"] = _compute_rolling_forecast(weekly, window)
    console.print(f"  Overall forecast: {FORECAST_HORIZON} weeks, window={window}")

    # Per-region forecasts if available
    if "region" in sales_df.columns:
        regional_fc = pd.DataFrame()
        for region in sales_df["region"].unique():
            region_weekly = (
                sales_df[sales_df["region"] == region]
                .set_index("transaction_date")
                .resample("W")["amount"]
                .sum()
                .reset_index()
            )
            region_weekly.columns = ["period", "total_amount"]
            fc = _compute_rolling_forecast(region_weekly, window)
            if not fc.empty:
                fc["region"] = region
                regional_fc = regional_fc.append(fc, ignore_index=True)

        results["by_region"] = regional_fc
        console.print(f"  Regional forecasts: {sales_df['region'].nunique()} regions")

    return results
