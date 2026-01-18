from app.clickhouse import clickhouse_client
from app.aggregation.rules import AggregationRule
import logging

logger = logging.getLogger(__name__)


class AggregationEngine:
    async def run_rule(self, rule: AggregationRule):
        client = await clickhouse_client.get_client()

        value, unit = rule.ch_interval
        metric = rule.metric
        group_col = rule.group_by[0]  # event_type

        metric_sql = {
            "event_count": "count()",
            "active_users": "uniqExact(user_id)",
        }.get(metric)

        if not metric_sql:
            raise ValueError(f"Unsupported metric: {metric}")

        sql = f"""
        INSERT INTO analytics.events_agg
        (
            rule_id,
            window_start,
            metric,
            group_key,
            value
        )
        SELECT
            %(rule_id)s                                         AS rule_id,
            toStartOfInterval(timestamp, INTERVAL {value} {unit}) AS window_start,
            %(metric)s                                          AS metric,
            {group_col}                                         AS group_key,
            {metric_sql}                                        AS value
        FROM analytics.events_fact
        GROUP BY
            window_start,
            group_key
        """

        params = {
            "rule_id": rule.rule_id,
            "metric": metric,
        }

        logger.info("🚀 Running aggregation rule: %s", rule.rule_id)
        await client.command(sql, parameters=params)
