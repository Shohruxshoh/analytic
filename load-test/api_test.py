import asyncio
import httpx
import time
import uuid
import random
from datetime import datetime, timezone

INGEST_URL = "http://ingestion-api:8000/ingest"
EVENT_TYPES = ["click", "view", "purchase"]
USER_IDS = [f"user_{i}" for i in range(1, 1001)]

TOTAL_REQUESTS = 10
BATCH_SIZE = 1000
CONCURRENT_REQUESTS = 10


def generate_event():
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": random.choice(USER_IDS),
        "event_type": random.choice(EVENT_TYPES),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "payload": {"v": random.randint(1, 100)},
    }


async def ingest_batch(client: httpx.AsyncClient, batch_id: int):
    events = [generate_event() for _ in range(BATCH_SIZE)]
    start = time.perf_counter()

    try:
        response = await client.post(INGEST_URL, json=events, timeout=30.0)
        response.raise_for_status()
        latency = (time.perf_counter() - start) * 1000
        print(f"Batch {batch_id + 1} | Latency: {latency:.2f} ms | Events: {len(events)}")
        return latency, len(events)
    except Exception as e:
        print(f"Batch {batch_id + 1} failed: {e}")
        return 0, 0


async def main():
    print(f"🚀 Async API load test started (Concurrency: {CONCURRENT_REQUESTS})\n")

    test_start = time.perf_counter()

    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)

    async with httpx.AsyncClient() as client:
        tasks = []

        async def limited_task(i):
            async with semaphore:
                return await ingest_batch(client, i)

        for i in range(TOTAL_REQUESTS):
            tasks.append(limited_task(i))

        results = await asyncio.gather(*tasks)

    total_time = time.perf_counter() - test_start
    latencies = [r[0] for r in results if r[0] > 0]
    total_events = sum(r[1] for r in results)

    print("\n📊 SUMMARY")
    print(f"Total requests: {TOTAL_REQUESTS}")
    print(f"Total events: {total_events}")
    print(f"Total time: {total_time:.2f} sec")
    if total_time > 0:
        print(f"Throughput: {int(total_events / total_time)} events/sec")
    if latencies:
        print(f"Avg latency: {sum(latencies) / len(latencies):.2f} ms")
        print(f"Max latency: {max(latencies):.2f} ms")


if __name__ == "__main__":
    asyncio.run(main())
