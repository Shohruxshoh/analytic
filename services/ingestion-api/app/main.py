from fastapi import FastAPI
from app.api.ingest import router as ingest_router
from app.lifespan import lifespan
from app.core.kafka import kafka_producer
from app.services.worker import kafka_worker

app = FastAPI(
    title="Ingestion API",
    lifespan=lifespan,
)

app.include_router(ingest_router)
