from fastapi import APIRouter, HTTPException
from typing import List
from app.schemas.event import EventSchema
from app.core.kafka import kafka_producer
from app.services.queue import event_queue

router = APIRouter()


@router.post("/ingest")
async def ingest_events(events: List[EventSchema]):
    if len(events) > 10_000:
        raise HTTPException(
            status_code=400,
            detail="Maximum 10,000 events per request",
        )

    if event_queue.full():
        raise HTTPException(
            status_code=503,
            detail="Ingest overloaded, try again later",
        )

    await event_queue.put(events)

    return {
        "status": "accepted",
        "count": len(events),
    }
