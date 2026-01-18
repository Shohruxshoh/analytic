import asyncio

event_queue: asyncio.Queue[list] = asyncio.Queue(maxsize=100)
