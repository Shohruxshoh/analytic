from datetime import datetime
from fastapi import APIRouter, HTTPException, status
from motor.motor_asyncio import AsyncIOMotorClient
from app.schemas.schemas import AggregationRuleResponse, AggregationRuleCreate
from app.core.config import settings
import logging
import socket

router = APIRouter(prefix="/aggregation-rule", tags=["Aggregation Rules"])
mongo = AsyncIOMotorClient(settings.MONGO_URI)
collection = mongo[settings.MONGO_DB].aggregation_rules

logger = logging.getLogger(__name__)

mongo_db = mongo[settings.MONGO_DB]


@router.post(
    "",
    response_model=dict,
    status_code=status.HTTP_201_CREATED,
    summary="Create aggregation rule (TEST MODE)",
)
async def create_rule(payload: AggregationRuleCreate):
    collection = mongo_db.aggregation_rules

    logger.warning("USING MONGO DB: %s", mongo_db.name)
    logger.warning(
        "CREATE_RULE → URI=%s | DB=%s | HOST=%s",
        settings.MONGO_URI,
        mongo_db.name,
        socket.gethostname(),
    )

    exists = await collection.find_one({"rule_id": payload.rule_id})
    if exists:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Rule with this rule_id already exists",
        )

    now = datetime.utcnow()

    doc = payload.model_dump()
    doc.update(
        {
            "is_active": True,
            "created_at": now,
            "updated_at": now,
        }
    )

    await collection.insert_one(doc)

    saved = await collection.find_one(
        {"rule_id": payload.rule_id},
        {"_id": 0},
    )

    return {
        "status": "created",
        "saved_from_db": saved,
        "db": mongo_db.name,
        "host": socket.gethostname(),
    }
