from datetime import datetime
from typing import Any
from pydantic import BaseModel, Field


class EventSchema(BaseModel):
    event_id: str
    user_id: str
    event_type: str
    timestamp: datetime
    payload: dict[str, Any] = Field(default_factory=dict)
