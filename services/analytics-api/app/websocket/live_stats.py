import json
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from app.websocket.ws import ConnectionManager
from app.websocket.rule_updater import RuleUpdater
from app.clickhouse import clickhouse_client

router = APIRouter()

manager = ConnectionManager()
updater = RuleUpdater(manager, clickhouse_client)

@router.websocket("/live-stats")
async def live_stats(websocket: WebSocket):
    await websocket.accept()

    try:

        raw = await websocket.receive_text()
        payload = json.loads(raw)

        rule_ids = payload.get("subscribe_rules")
        if not rule_ids:
            await websocket.close(code=1008)
            return

        await manager.connect(websocket, rule_ids)
        for rule_id in rule_ids:
            await updater.ensure_rule_task(rule_id)

        while True:
            await websocket.receive_text()

    except WebSocketDisconnect:
        manager.disconnect(websocket)

    except Exception as e:
        manager.disconnect(websocket)
        raise e
