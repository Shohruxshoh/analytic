import json
from datetime import datetime
from typing import Dict, Set
from fastapi import WebSocket


class ConnectionManager:
    def __init__(self):
        self.rule_connections: Dict[str, Set[WebSocket]] = {}

    async def connect(self, websocket: WebSocket, rule_ids: list[str]):
        for rule_id in rule_ids:
            self.rule_connections.setdefault(rule_id, set()).add(websocket)

    def disconnect(self, websocket: WebSocket):
        for rule_id in list(self.rule_connections.keys()):
            self.rule_connections[rule_id].discard(websocket)
            if not self.rule_connections[rule_id]:
                del self.rule_connections[rule_id]

    async def broadcast(self, rule_id: str, payload: dict):
        if rule_id not in self.rule_connections:
            return

        message = json.dumps({
            "rule_id": rule_id,
            "timestamp": datetime.utcnow().isoformat(),
            "data": payload,
        })

        dead = set()
        for ws in self.rule_connections[rule_id]:
            try:
                await ws.send_text(message)
            except Exception:
                dead.add(ws)

        for ws in dead:
            self.disconnect(ws)

    def has_subscribers(self, rule_id: str) -> bool:
        return rule_id in self.rule_connections
