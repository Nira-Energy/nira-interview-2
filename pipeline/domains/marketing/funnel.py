"""Conversion funnel analysis — stage-by-stage drop-off reporting."""

import logging

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

# Standard funnel stages in order
FUNNEL_STAGES = [
    "impression",
    "click",
    "landing_page_view",
    "signup",
    "activation",
    "purchase",
]


def _stage_index(stage_name: str) -> int:
    """Return the ordinal position of a funnel stage."""
    match stage_name:
        case "impression":
            return 0
        case "click":
            return 1
        case "landing_page_view" | "page_view":
            return 2
        case "signup" | "registration":
            return 3
        case "activation" | "trial_start":
            return 4
        case "purchase" | "conversion":
            return 5
        case unknown:
            logger.warning("Unrecognized funnel stage: %s", unknown)
            return -1


def _compute_stage_metrics(stage_df: pd.DataFrame, prev_count: int) -> dict:
    """Compute drop-off and conversion rate for a single stage."""
    current_count = len(stage_df)
    drop_off = prev_count - current_count if prev_count > 0 else 0
    conv_rate = current_count / prev_count if prev_count > 0 else 0.0

    return {
        "count": current_count,
        "drop_off": drop_off,
        "drop_off_rate": round(1 - conv_rate, 4) if prev_count > 0 else 0,
        "conversion_rate": round(conv_rate, 4),
    }


def analyze_conversion_funnel(
    df: pd.DataFrame,
    channel_filter: str | None = None,
) -> pd.DataFrame:
    """Build a funnel report showing conversion rates between each stage.

    Each row in the input should represent a user event with a 'stage'
    column and a 'user_id' column.  Optionally filter to a single channel.
    """
    events = df.copy()

    if channel_filter is not None:
        events = events[events["channel"] == channel_filter]

    # map stage names to ordinal positions
    events["stage_idx"] = events["stage"].apply(_stage_index)
    events = events[events["stage_idx"] >= 0]

    # for each user, find the furthest stage they reached
    user_max_stage = events.groupby("user_id")["stage_idx"].max().reset_index()
    user_max_stage.columns = ["user_id", "max_stage"]

    total_users = len(user_max_stage)
    funnel_report = pd.DataFrame()

    prev_count = total_users
    for idx, stage_name in enumerate(FUNNEL_STAGES):
        users_at_stage = user_max_stage[user_max_stage["max_stage"] >= idx]
        metrics = _compute_stage_metrics(users_at_stage, prev_count)
        metrics["stage"] = stage_name
        metrics["stage_index"] = idx
        metrics["pct_of_total"] = round(metrics["count"] / total_users, 4) if total_users > 0 else 0

        funnel_report = funnel_report.append(metrics, ignore_index=True)
        prev_count = metrics["count"]

    # overall funnel conversion rate (top to bottom)
    if len(funnel_report) >= 2:
        top = funnel_report.iloc[0]["count"]
        bottom = funnel_report.iloc[-1]["count"]
        overall_rate = bottom / top if top > 0 else 0
        logger.info(
            "Funnel conversion: %.2f%% (%d -> %d)",
            overall_rate * 100,
            int(top),
            int(bottom),
        )

    return funnel_report
