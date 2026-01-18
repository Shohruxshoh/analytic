import logging
from app.services.queue import event_queue
from app.core.kafka import kafka_producer

logger = logging.getLogger(__name__)


async def kafka_worker():
    while True:
        events = await event_queue.get()

        try:
            payload = [e.model_dump(mode="json") for e in events]
            await kafka_producer.send_batch(payload)

        except Exception:
            logger.exception("❌ Kafka worker failed")

        finally:
            event_queue.task_done()

