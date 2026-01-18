import asyncio
from app.consumer import event_consumer
from app.core.logging import setup_logging
from app.aggregation.scheduler import AggregationScheduler
from app.clickhouse import clickhouse_client
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



async def main():
    scheduler = AggregationScheduler()

    logger.info("🚀 Services starting...")

    # 1️⃣ Kafka consumer start
    await event_consumer.start()

    # 2️⃣ TASKLARNI ALOHIDA START QILAMIZ
    consumer_task = asyncio.create_task(
        event_consumer.consume(),
        name="kafka-consumer",
    )

    scheduler_task = asyncio.create_task(
        scheduler.run(),
        name="aggregation-scheduler",
    )

    logger.info("📡 Ingestion & Aggregation running")

    try:
        # 3️⃣ IKKALASINI KUZATAMIZ
        done, pending = await asyncio.wait(
            [consumer_task, scheduler_task],
            return_when=asyncio.FIRST_EXCEPTION,
        )

        for task in done:
            if task.exception():
                raise task.exception()

    except asyncio.CancelledError:
        logger.info("🛑 Shutdown requested")

    except Exception as e:
        logger.exception(f"💥 Fatal error: {e}")

    finally:
        logger.info("🧹 Shutting down services...")

        # TASKLARNI TO‘XTATAMIZ
        consumer_task.cancel()
        scheduler_task.cancel()

        await asyncio.gather(
            consumer_task,
            scheduler_task,
            return_exceptions=True,
        )

        await event_consumer.stop()

        client = await clickhouse_client.get_client()
        await client.close()

        logger.info("✅ Shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
