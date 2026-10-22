from uuid import uuid4

import pytest
from pydantic import ValidationError

from realtime_recommendation_pipeline.common.events import ViewingEvent


@pytest.fixture
def valid_event() -> dict[str, object]:
    return {
        "event_id": str(uuid4()),
        "schema_version": 1,
        "user_id": "user-001",
        "programme_id": "programme-001",
        "event_type": "play_start",
        "position_s": 0,
        "duration_s": 1800,
        "device": "smart_tv",
        "event_time": 1790000000000,
        "produced_at": 1790000000025,
    }


def test_valid_event_is_parsed(valid_event: dict[str, object]) -> None:
    event = ViewingEvent.model_validate(valid_event)

    assert str(event.event_id) == valid_event["event_id"]
    assert event.event_type == "play_start"
    assert event.schema_version == 1


@pytest.mark.parametrize(
    "field",
    [
        "event_id",
        "user_id",
        "programme_id",
        "event_type",
        "position_s",
        "duration_s",
        "device",
        "event_time",
        "produced_at",
    ],
)
def test_missing_required_field_is_rejected(
    valid_event: dict[str, object],
    field: str,
) -> None:
    del valid_event[field]

    with pytest.raises(ValidationError):
        ViewingEvent.model_validate(valid_event)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("event_id", "not-a-uuid"),
        ("schema_version", 2),
        ("event_type", "rewind"),
        ("position_s", -1),
        ("duration_s", -10),
        ("event_time", "not-a-timestamp"),
        ("produced_at", "not-a-timestamp"),
        ("user_id", ""),
        ("programme_id", ""),
        ("device", ""),
        ("unexpected_field", "not-allowed"),
    ],
)
def test_invalid_values_are_rejected(
    valid_event: dict[str, object],
    field: str,
    value: object,
) -> None:
    valid_event[field] = value

    with pytest.raises(ValidationError):
        ViewingEvent.model_validate(valid_event)


def test_unknown_duration_is_allowed(
    valid_event: dict[str, object],
) -> None:
    valid_event["duration_s"] = None

    event = ViewingEvent.model_validate(valid_event)

    assert event.duration_s is None
