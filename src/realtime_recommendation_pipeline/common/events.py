from enum import Enum
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

NonEmptyString = Annotated[str, Field(min_length=1)]
TimestampMs = Annotated[int, Field(ge=0)]
Seconds = Annotated[float, Field(ge=0)]


class ViewingEventType(Enum):
    PLAY_START = "play_start"
    HEARTBEAT = "heartbeat"
    PAUSE = "pause"
    PLAY_END = "play_end"


class ViewingEvent(BaseModel):
    """Validated event describing synthetic programme viewing activity."""

    model_config = ConfigDict(extra="forbid")

    event_id: UUID
    schema_version: Literal[1] = 1
    user_id: NonEmptyString
    programme_id: NonEmptyString
    event_type: ViewingEventType
    position_s: Seconds
    duration_s: Seconds | None
    device: NonEmptyString
    event_time: TimestampMs
    produced_at: TimestampMs
