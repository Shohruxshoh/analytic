import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.services.worker import kafka_worker
from app.core.kafka import kafka_producer
from app.core.logging import setup_logging


@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_logging()
    await kafka_producer.start()
    asyncio.create_task(kafka_worker())
    yield
    await kafka_producer.stop()
