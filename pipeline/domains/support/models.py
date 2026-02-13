"""Pandera schemas for validating support domain DataFrames."""

import pandera as pa
from pandera import Column, Check, Index

VALID_PRIORITIES = ["critical", "high", "medium", "low"]
VALID_STATUSES = ["open", "in_progress", "pending", "resolved", "reopened", "unknown"]
VALID_SOURCES = ["zendesk", "intercom", "email_inbox"]


TicketSchema = pa.DataFrameSchema(
    columns={
        "ticket_id": Column(str, Check.str_matches(r"^[A-Z]{2,4}-\d+$"), unique=True),
        "created_at": Column("datetime64[ns]", nullable=False),
        "resolved_at": Column("datetime64[ns]", nullable=True),
        "priority": Column(str, Check.isin(VALID_PRIORITIES)),
        "status": Column(str, Check.isin(VALID_STATUSES)),
        "agent_id": Column(str, nullable=True),
        "source_system": Column(str, Check.isin(VALID_SOURCES)),
        "subject": Column(str, nullable=False),
        "resolution_hours": Column(float, Check.ge(0), nullable=True),
    },
    coerce=True,
    strict=False,
)


AgentSchema = pa.DataFrameSchema(
    columns={
        "agent_id": Column(str, Check.str_length(min_value=1), unique=True),
        "name": Column(str, nullable=False),
        "team": Column(str, nullable=False),
        "total_tickets": Column(int, Check.ge(0), nullable=True),
        "avg_weekly_load": Column(float, Check.ge(0), nullable=True),
        "quality_score": Column(float, Check.in_range(0, 100), nullable=True),
    },
    coerce=True,
    strict=False,
)


SLAComplianceSchema = pa.DataFrameSchema(
    columns={
        "ticket_id": Column(str, nullable=False),
        "priority": Column(str, Check.isin(VALID_PRIORITIES)),
        "response_met": Column(bool, nullable=True),
        "resolution_met": Column(bool, nullable=True),
        "response_target_hrs": Column(float, Check.gt(0)),
        "resolution_target_hrs": Column(float, Check.gt(0)),
    },
    coerce=True,
    strict=False,
)


EscalationSchema = pa.DataFrameSchema(
    columns={
        "ticket_id": Column(str, nullable=False),
        "times_escalated": Column(int, Check.ge(1)),
        "esc_reason": Column(str, nullable=False),
        "esc_severity": Column(str, Check.isin(["red", "orange", "yellow", "green"])),
    },
    coerce=True,
    strict=False,
)
