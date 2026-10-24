import json
from uuid import uuid4

import pytest

from realtime_recommendation_pipeline.flink_jobs.validation import validate_event


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


def test_valid_event_passes(valid_event: dict[str, object]) -> None:
    result = validate_event(json.dumps(valid_event))

    assert result.event is not None
    assert result.dead_letter is None
    assert str(result.event.event_id) == valid_event["event_id"]


def test_bad_timestamp_is_dead_lettered(valid_event: dict[str, object]) -> None:
    valid_event["event_time"] = "not-a-timestamp"
    raw = json.dumps(valid_event)

    result = validate_event(raw)

    assert result.event is None
    assert result.dead_letter is not None
    assert result.dead_letter["error_type"] == "schema_violation"
    assert "event_time" in result.dead_letter["reason"]
    assert result.dead_letter["raw"] == raw


def test_extra_field_is_dead_lettered(valid_event: dict[str, object]) -> None:
    valid_event["unexpected_field"] = "not-allowed"

    result = validate_event(json.dumps(valid_event))

    assert result.dead_letter is not None
    assert "unexpected_field" in result.dead_letter["reason"]


@pytest.mark.parametrize("raw", ["{not json", "", "hello"])
def test_invalid_json_is_dead_lettered(raw: str) -> None:
    result = validate_event(raw)

    assert result.event is None
    assert result.dead_letter is not None
    assert result.dead_letter["error_type"] == "invalid_json"


@pytest.mark.parametrize("raw", ["[]", "null", '"x"', "42"])
def test_non_object_json_is_dead_lettered(raw: str) -> None:
    result = validate_event(raw)

    assert result.dead_letter is not None
    assert result.dead_letter["error_type"] == "schema_violation"


def test_dead_letter_is_json_serialisable(valid_event: dict[str, object]) -> None:
    valid_event["event_time"] = "not-a-timestamp"

    result = validate_event(json.dumps(valid_event))

    assert result.dead_letter is not None
    assert json.loads(json.dumps(result.dead_letter)) == result.dead_letter
