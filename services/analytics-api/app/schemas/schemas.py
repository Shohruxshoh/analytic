from pydantic import BaseModel, Field
from typing import List, Literal
from datetime import datetime


class AggregationRuleCreate(BaseModel):
    rule_id: str = Field(
        ...,
        example="rule_10m_event_type",
        description="Unique aggregation rule identifier",
    )
    window_size: Literal["1m", "5m", "10m", "1h"] = Field(
        ...,
        example="10m",
    )
    metric: Literal["event_count", "active_users"] = Field(
        ...,
        example="event_count",
    )
    group_by: List[str] = Field(
        ...,
        example=["event_type"],
        min_items=1,
    )
    top_n: int | None = Field(
        None,
        example=10,
        description="Optional top N per window",
    )


class AggregationRuleResponse(BaseModel):
    status: str = "created"
    rule_id: str
    created_at: datetime



class StatsItem(BaseModel):
    window_start: datetime = Field(
        ...,
        description="Aggregation window start time",
        example="2026-01-16T18:00:00",
    )
    group_key: str = Field(
        ...,
        description="Group key (e.g. event_type=purchase)",
        example="event_type=purchase",
    )
    value: int = Field(
        ...,
        description="Aggregated metric value",
        example=102424,
    )


class StatsResponse(BaseModel):
    rule_id: str = Field(
        ...,
        example="rule_10m_event_type",
    )
    from_time: datetime = Field(
        ...,
        alias="from",
        example="2026-01-16T18:00:00",
    )
    to_time: datetime = Field(
        ...,
        alias="to",
        example="2026-01-16T19:00:00",
    )
    count: int = Field(
        ...,
        example=3,
    )
    data: List[StatsItem]