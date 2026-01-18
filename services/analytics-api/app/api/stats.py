from datetime import datetime
from fastapi import APIRouter, Query, HTTPException
from typing import Optional
from app.schemas.schemas import StatsResponse
from app.clickhouse import clickhouse_client
from datetime import timezone

router = APIRouter(prefix="/stats", tags=["Stats"])

@router.get(
    "",
    summary="Get aggregated statistics",
    description=(
        "Returns aggregated analytics data from ClickHouse.\n\n"
        "- Uses pre-aggregated tables (events_agg)\n"
        "- Safe for ReplacingMergeTree\n"
        "- Stable results (no double counting)\n"
        "- Optimized for <300ms with 100M+ rows"
    ),
)
async def get_stats(
    rule_id: str = Query(..., description="Aggregation rule ID"),
    start_time: datetime = Query(..., description="Start time (ISO 8601)"),
    end_time: datetime = Query(..., description="End time (ISO 8601)"),
    event_type: Optional[str] = Query(None, description="Optional event_type filter"),
    limit: int = Query(100, ge=1, le=1000),
):

    if start_time.tzinfo is None:
        start_time = start_time.replace(tzinfo=timezone.utc)
    if end_time.tzinfo is None:
        end_time = end_time.replace(tzinfo=timezone.utc)

    if start_time >= end_time:
        raise HTTPException(400, "start_time must be < end_time")

    query = """
    SELECT
        window_start,
        group_key,
        anyLast(value) AS value
    FROM analytics.events_agg
    PREWHERE
        rule_id = %(rule_id)s
        AND metric = 'event_count'
        AND window_start >= %(start)s
        AND window_start < %(end)s
    """

    params = {
        "rule_id": rule_id,
        "start": start_time,
        "end": end_time,
        "limit": limit,
    }

    if event_type:
        query += " AND group_key = %(group_key)s"
        params["group_key"] = event_type

    query += """
    GROUP BY
        window_start,
        group_key
    ORDER BY
        window_start DESC
    LIMIT %(limit)s
    """

    rows = await clickhouse_client.query(query, params)

    return {
        "rule_id": rule_id,
        "from": start_time.isoformat(),
        "to": end_time.isoformat(),
        "count": len(rows),
        "data": [
            {
                "window_start": r[0].isoformat(),
                "event_type": r[1],
                "value": r[2],
            }
            for r in rows
        ],
    }