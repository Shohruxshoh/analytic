from fastapi import FastAPI

from app.api.aggregation_rule import router as aggregation_router
from app.api.stats import router as stats_router
from app.websocket.live_stats import router as ws_router

app = FastAPI(title="Analytics API")

app.include_router(aggregation_router)
app.include_router(stats_router)
app.include_router(ws_router)
