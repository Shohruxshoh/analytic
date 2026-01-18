import asyncio
import orjson  # pip install orjson - standart json dan 5x tezroq
import uuid
import time
import random
from datetime import datetime, timezone
from aiokafka import AIOKafkaProducer

KAFKA_BOOTSTRAP_SERVERS = "kafka-1:9092,kafka-2:9092,kafka-3:9092"
TOPIC = "events_raw"

DURATION_SECONDS = 60
PRODUCER_WORKERS = 5


async def producer_worker(producer: AIOKafkaProducer, worker_id: int):
    sent = 0
    start = time.time()

    event_types = ["click", "view", "purchase", "login"]

    while time.time() - start < DURATION_SECONDS:
        tasks = []
        # Kichik paketlarda (batch) yuborish loopni tezlashtiradi
        for _ in range(500):
            event = {
                "event_id": str(uuid.uuid4()),
                "user_id": random.randint(1, 100_000),
                "event_type": random.choice(event_types),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            tasks.append(producer.send(TOPIC, value=event))

        await asyncio.gather(*tasks)
        sent += len(tasks)

    print(f"🧵 Worker {worker_id} sent {sent} events")
    return sent


async def main():
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: orjson.dumps(v),
        linger_ms=50,
        acks=1,
        compression_type="lz4",
        max_batch_size=16384 * 4,
        retry_backoff_ms=500
    )

    await producer.start()
    print(f"🚀 Kafka load producer started (Target: 100k msg/min)")

    start_time = time.time()

    try:
        results = await asyncio.gather(
            *[producer_worker(producer, i + 1) for i in range(PRODUCER_WORKERS)]
        )
    finally:
        await producer.stop()

    elapsed = time.time() - start_time
    total_sent = sum(results)

    events_per_sec = total_sent / elapsed
    events_per_min = events_per_sec * 60

    print("\n" + "=" * 30)
    print("✅ LOAD TEST FINISHED")
    print(f"Total Events: {total_sent}")
    print(f"Duration: {elapsed:.2f} sec")
    print(f"Throughput (Sec): {int(events_per_sec)} events/sec")
    print(f"Throughput (Min): {int(events_per_min):,} events/min")
    print("=" * 30)

    if events_per_min >= 100000:
        print("🏆 Natija: MUVAFFAQIYATLI (100k marrasi urildi!)")
    else:
        print("⚠️ Natija: MAQSADGA YETILMADI (Partitsiyalarni tekshiring)")


if __name__ == "__main__":
    asyncio.run(main())