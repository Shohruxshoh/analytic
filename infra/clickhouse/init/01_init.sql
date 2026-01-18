CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.events_fact
(
    event_id String,
    user_id String,
    event_type String,
    timestamp DateTime
)
ENGINE = MergeTree
PARTITION BY toDate(timestamp)
ORDER BY (event_type, timestamp);

CREATE TABLE IF NOT EXISTS analytics.events_agg
(
    rule_id String,
    window_start DateTime,
    metric String,
    group_key String,
    value UInt64
)
ENGINE = ReplacingMergeTree()
PARTITION BY toDate(window_start)
ORDER BY (rule_id, window_start, metric, group_key);

CREATE TABLE IF NOT EXISTS analytics.aggregation_state
(
    rule_id String,
    last_window_start DateTime
)
ENGINE = ReplacingMergeTree()
ORDER BY rule_id;
