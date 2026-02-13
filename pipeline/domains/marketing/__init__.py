"""Marketing domain — campaign analytics, attribution, and ROI reporting."""

from pipeline.domains.marketing.ingest import ingest_campaign_data
from pipeline.domains.marketing.transform import normalize_campaigns
from pipeline.domains.marketing.campaigns import analyze_campaign_performance
from pipeline.domains.marketing.attribution import compute_attribution
from pipeline.domains.marketing.channels import compare_channels
from pipeline.domains.marketing.segments import build_audience_segments
from pipeline.domains.marketing.roi import calculate_campaign_roi
from pipeline.domains.marketing.funnel import analyze_conversion_funnel
from pipeline.domains.marketing.models import CampaignSchema, ChannelSchema


def validate(df, schema_name: str = "campaign") -> bool:
    """Validate marketing data against the appropriate pandera schema."""
    match schema_name:
        case "campaign":
            CampaignSchema.validate(df)
        case "channel":
            ChannelSchema.validate(df)
        case other:
            raise ValueError(f"No schema registered for: {other}")
    return True


def run(channels: list[str] | None = None, lookback_days: int = 90):
    """Execute the full marketing analytics pipeline."""
    raw = ingest_campaign_data(channels=channels, lookback_days=lookback_days)
    cleaned = normalize_campaigns(raw)
    validate(cleaned, "campaign")

    performance = analyze_campaign_performance(cleaned)
    attribution = compute_attribution(cleaned)
    channel_report = compare_channels(cleaned)
    segments = build_audience_segments(cleaned)
    roi = calculate_campaign_roi(cleaned)
    funnel = analyze_conversion_funnel(cleaned)

    return {
        "campaign_performance": performance,
        "attribution": attribution,
        "channel_comparison": channel_report,
        "audience_segments": segments,
        "roi_analysis": roi,
        "funnel_report": funnel,
    }
