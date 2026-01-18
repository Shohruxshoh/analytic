from motor.motor_asyncio import AsyncIOMotorClient
from app.core.settings import settings

class MongoClient:
    def __init__(self):
        self.client = AsyncIOMotorClient(settings.MONGO_URI)
        self.db = self.client[settings.MONGO_DB]
        self.collection = self.db.events_raw

    async def insert_many(self, events: list[dict]):
        if events:
            await self.collection.insert_many(events)


mongo_client = MongoClient()
