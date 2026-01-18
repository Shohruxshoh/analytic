import clickhouse_connect
from datetime import datetime
from app.core.settings import settings


class ClickHouseClient:
    def __init__(self):
        self._client = None

    async def get_client(self):
        if self._client is None:
            self._client = await clickhouse_connect.get_async_client(
                host=settings.CLICKHOUSE_HOST,
                port=settings.CLICKHOUSE_PORT,
                username=settings.CLICKHOUSE_USER,
                password=settings.CLICKHOUSE_PASSWORD,
                database="analytics"
            )
        return self._client

    async def insert_events(self, events: list[dict]):
        if not events:
            return

        client = await self.get_client()

        rows = []
        for e in events:
            ts = e["timestamp"]
            if isinstance(ts, str):
                ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))

            rows.append([
                str(e["event_id"]),
                str(e["user_id"]),
                str(e["event_type"]),
                ts
            ])

        try:
            await client.insert(
                "events_fact",
                rows,
                column_names=["event_id", "user_id", "event_type", "timestamp"]
            )
        except Exception as e:
            print(f"❌ ClickHouse Async Insert Error: {e}")
            self._client = None

clickhouse_client = ClickHouseClient()
