import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime, timedelta
from app.core.settings import settings
from app.aggregation.rules import AggregationRule
from app.aggregation.engine import AggregationEngine
import logging
import socket
import uuid

logger = logging.getLogger(__name__)


class AggregationScheduler:
    LOCK_ID = "aggregation_scheduler_lock"
    LOCK_TTL_SECONDS = 30

    def __init__(self):
        self.engine = AggregationEngine()

        self.mongo = AsyncIOMotorClient(settings.MONGO_URI)
        self.db = self.mongo[settings.MONGO_DB]
        logger.warning("SCHEDULER USING DB: %s", settings.MONGO_DB)
        self.rules_col = self.db.aggregation_rules
        self.lock_col = self.db.aggregation_locks

        self.owner_id = f"{socket.gethostname()}-{uuid.uuid4()}"

    async def acquire_lock(self) -> bool:
        now = datetime.utcnow()
        expires_at = now + timedelta(seconds=self.LOCK_TTL_SECONDS)

        res = await self.lock_col.find_one_and_update(
            {
                "_id": self.LOCK_ID,
                "$or": [
                    {"expires_at": {"$lt": now}},
                    {"owner_id": self.owner_id},
                ],
            },
            {
                "$set": {
                    "expires_at": expires_at,
                    "owner_id": self.owner_id,
                    "updated_at": now,
                }
            },
            upsert=True,
            return_document=True,
        )

        return res is not None

    async def release_lock(self):
        await self.lock_col.update_one(
            {"_id": self.LOCK_ID, "owner_id": self.owner_id},
            {"$set": {"expires_at": datetime.utcnow()}},
        )

    async def fetch_active_rules(self) -> list[AggregationRule]:
        rules: list[AggregationRule] = []
        logger.warning(f"143 - {self.rules_col}", )

        async for doc in self.rules_col.find({"is_active": True}):
            try:
                rules.append(
                    AggregationRule(
                        rule_id=doc["rule_id"],
                        window_size=doc["window_size"],
                        metric=doc["metric"],
                        group_by=doc["group_by"],
                        top_n=doc.get("top_n"),
                    )
                )
            except KeyError as e:
                logger.warning("Invalid rule skipped: %s (%s)", doc, e)

        return rules

    async def run(self):
        logger.info(
            "📊 Aggregation scheduler started (owner=%s)",
            self.owner_id,
        )

        while True:
            try:
                has_lock = await self.acquire_lock()
                if not has_lock:
                    await asyncio.sleep(3)
                    continue

                rules = await self.fetch_active_rules()
                if not rules:
                    logger.info("No active aggregation rules")

                for rule in rules:
                    await self.engine.run_rule(rule)

            except Exception:
                logger.exception("❌ Aggregation scheduler failure")

            finally:
                await self.release_lock()

            await asyncio.sleep(10)
