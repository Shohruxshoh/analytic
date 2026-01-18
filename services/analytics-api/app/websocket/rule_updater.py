import asyncio
import logging
from app.core.config import settings

logger = logging.getLogger(__name__)


class RuleUpdater:
    def __init__(self, manager, ch_client):
        self.manager = manager
        self.ch = ch_client
        self.tasks: dict[str, asyncio.Task] = {}

    async def ensure_rule_task(self, rule_id: str):
        if rule_id in self.tasks:
            return

        async def loop():
            try:
                while True:
                    if not self.manager.has_subscribers(rule_id):
                        break

                    data = await self.ch.fetch_rule_stats(rule_id)
                    if data:
                        await self.manager.broadcast(rule_id, data)

                    await asyncio.sleep(settings.WS_PUSH_INTERVAL)
            finally:
                self.tasks.pop(rule_id, None)

        self.tasks[rule_id] = asyncio.create_task(loop())
