from dataclasses import dataclass

WINDOW_TO_CH_INTERVAL = {
    "1m": (1, "MINUTE"),
    "5m": (5, "MINUTE"),
    "10m": (10, "MINUTE"),
    "1h": (1, "HOUR"),
}

@dataclass
class AggregationRule:
    rule_id: str
    window_size: str
    metric: str
    group_by: list[str]
    top_n: int | None = None

    @property
    def ch_interval(self):
        return WINDOW_TO_CH_INTERVAL[self.window_size]
