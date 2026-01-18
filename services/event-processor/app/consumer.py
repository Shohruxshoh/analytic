import json
import logging
from datetime import datetime
from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError
from app.core.settings import settings
from app.mongo import mongo_client
from app.clickhouse import clickhouse_client

logger = logging.getLogger(__name__)


class EventConsumer:
    def __init__(self):
        self.consumer: AIOKafkaConsumer | None = None
        self._running = False

    async def start(self):
        self.consumer = AIOKafkaConsumer(
            settings.KAFKA_TOPIC,
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            group_id=settings.KAFKA_GROUP_ID,

            enable_auto_commit=False,
            auto_offset_reset="earliest",

            session_timeout_ms=30000,
            heartbeat_interval_ms=10000,

            value_deserializer=lambda x: x,
        )

        await self.consumer.start()
        self._running = True
        logger.info("✅ Kafka consumer started")

    async def stop(self):
        self._running = False
        if self.consumer:
            await self.consumer.stop()
            logger.info("🛑 Kafka consumer stopped")

    async def consume(self):
        assert self.consumer is not None

        buffer: list[dict] = []

        async for msg in self.consumer:
            try:
                event = self._parse_message(msg.value)

                if event is None:
                    await self.consumer.commit()
                    continue

                buffer.append(event)

                if len(buffer) >= settings.BATCH_SIZE:
                    await self._handle_batch(buffer)
                    buffer.clear()
                    await self.consumer.commit()

            except Exception:
                logger.exception("💥 Unexpected consumer error")
                break

    @staticmethod
    def _parse_message(raw: bytes) -> dict | None:
        if not raw:
            logger.warning("⚠️ Empty Kafka message skipped")
            return None

        try:
            obj = json.loads(raw.decode("utf-8"))
            if not isinstance(obj, dict):
                logger.warning("⚠️ Non-dict JSON skipped: %r", obj)
                return None
            return obj
        except json.JSONDecodeError:
            logger.error("❌ Invalid JSON skipped")
            return None

    async def _handle_batch(self, events: list[dict]):

        valid_events = [e for e in events if isinstance(e, dict)]

        if not valid_events:
            logger.warning("⚠️ Empty valid batch, skipping")
            return

        logger.info("📦 Processing batch: %d events", len(valid_events))

        await mongo_client.insert_many(valid_events)

        # 2️⃣ ClickHouse
        ch_events = []
        for e in valid_events:
            ts = self._parse_timestamp(e.get("timestamp"))
            if not ts:
                continue

            ch_events.append({
                **e,
                "timestamp": ts,
            })

        if ch_events:
            await clickhouse_client.insert_events(ch_events)

    @staticmethod
    def _parse_timestamp(ts: str | None) -> datetime | None:
        if not ts:
            logger.warning("⚠️ Missing timestamp")
            return None

        try:
            # ISO 8601 → datetime
            return datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except Exception:
            logger.warning("⚠️ Invalid timestamp skipped: %s", ts)
            return None


event_consumer = EventConsumer()
