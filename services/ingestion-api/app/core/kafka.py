import asyncio
import logging
import orjson
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaTimeoutError, KafkaConnectionError
from app.core.config import settings

logger = logging.getLogger(__name__)


class KafkaProducer:
    def __init__(self):
        self._producer: AIOKafkaProducer | None = None

        self.max_retries = 3
        self.retry_backoff = 0.5

        self.max_inflight = 1000
        self._semaphore = asyncio.Semaphore(self.max_inflight)

    async def start(self):
        self._producer = AIOKafkaProducer(
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,

            value_serializer=lambda v: orjson.dumps(v),

            acks="all",
            enable_idempotence=True,

            linger_ms=50,
            compression_type="lz4",
            max_request_size=5 * 1024 * 1024,

            retry_backoff_ms=500,
            request_timeout_ms=30_000,
        )

        await self._producer.start()
        logger.info("✅ Kafka producer started")

    async def _send_one(self, event: dict):

        async with self._semaphore:
            await self._producer.send_and_wait(
                topic=settings.KAFKA_TOPIC,
                key=str(event["user_id"]).encode(),
                value=event,
            )

    async def send_batch(self, events: list[dict]):
        if not self._producer or not events:
            return

        attempt = 0
        while attempt < self.max_retries:
            try:
                tasks = [self._send_one(e) for e in events]
                await asyncio.gather(*tasks)
                return  # ✅ success

            except (KafkaTimeoutError, KafkaConnectionError) as e:
                attempt += 1
                logger.warning(
                    f"Kafka batch send failed "
                    f"(attempt {attempt}/{self.max_retries}): {e}"
                )
                await asyncio.sleep(self.retry_backoff * attempt)

            except Exception:
                logger.exception("❌ Unexpected Kafka error")
                return

    async def stop(self):
        if self._producer:
            await self._producer.stop()
            logger.info("🛑 Kafka producer stopped")


kafka_producer = KafkaProducer()
