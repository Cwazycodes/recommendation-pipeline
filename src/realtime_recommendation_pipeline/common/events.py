"""The viewing event, as the jobs accept it.

The Avro schemas (schemas/viewing_event/) fix the fields and their types; this
model adds the rules Avro cannot express, such as a UUID event_id, non-empty
ids and no negative times.
"""

from enum import Enum
from typing import Annotated
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

NonEmptyString = Annotated[str, Field(min_length=1)]
TimestampMs = Annotated[int, Field(ge=0)]
Seconds = Annotated[float, Field(ge=0)]


class ViewingEventType(str, Enum):
    PLAY_START = "play_start"
    HEARTBEAT = "heartbeat"
    PAUSE = "pause"
    PLAY_END = "play_end"


class ViewingEvent(BaseModel):
    """Validated event describing synthetic programme viewing activity."""

    model_config = ConfigDict(extra="forbid")

    event_id: UUID
    user_id: NonEmptyString
    programme_id: NonEmptyString
    event_type: ViewingEventType
    position_s: Seconds
    duration_s: Seconds | None
    device: NonEmptyString
    event_time: TimestampMs
    produced_at: TimestampMs
    # New in v2 of the schema. The jobs read events as v1, so they never see it.
    session_id: NonEmptyString | None = None
