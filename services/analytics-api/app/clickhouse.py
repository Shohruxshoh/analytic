import clickhouse_connect
from app.core.config import settings


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
                database="analytics",
            )
        return self._client

    async def fetch_rule_stats(self, rule_id: str, top_n: int = 5):
        client = await self.get_client()

        main_query = """
        SELECT
            max(window_start) AS window_start,
            sum(event_count) AS event_count,
            max(active_users) AS active_users
        FROM analytics.events_agg
        WHERE rule_id = %(rule_id)s
        """

        top_events_query = """
        SELECT
            event_type,
            sum(event_count) AS cnt
        FROM analytics.events_agg
        WHERE rule_id = %(rule_id)s
        GROUP BY event_type
        ORDER BY cnt DESC
        LIMIT %(top_n)s
        """

        main = await client.query(main_query, parameters={"rule_id": rule_id})
        top_events = await client.query(
            top_events_query,
            parameters={"rule_id": rule_id, "top_n": top_n},
        )

        if not main.result_rows:
            return None

        row = main.result_rows[0]

        return {
            "window_start": row[0].isoformat() if row[0] else None,
            "event_count": row[1],
            "active_users": row[2],
            "top_events": [
                {"event_type": r[0], "count": r[1]}
                for r in top_events.result_rows
            ],
        }

    async def query(self, sql: str, params: dict):
        client = await self.get_client()
        result = await client.query(sql, parameters=params)
        return result.result_rows

clickhouse_client = ClickHouseClient()
