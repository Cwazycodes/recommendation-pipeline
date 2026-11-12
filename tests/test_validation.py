import base64
import json
from uuid import uuid4

import pytest

from realtime_recommendation_pipeline.common.event_schemas import (
    WriterSchemas,
    encode,
    parsed_schema,
    schema_text,
)
from realtime_recommendation_pipeline.common.schema_registry import (
    RegistryUnavailable,
    UnknownSchema,
)
from realtime_recommendation_pipeline.flink_jobs.validation import (
    KAFKA_VALUE_CHARSET,
    validate_event,
)

# The ids the registry gave each version, in these tests.
IDS = {1: 11, 2: 12}


def registry_schemas(registered: dict[int, str]) -> WriterSchemas:
    def fetch(schema_id: int) -> str:
        if schema_id not in registered:
            raise UnknownSchema(f"no schema with id {schema_id}")
        return registered[schema_id]

    return WriterSchemas(fetch)


SCHEMAS = registry_schemas({IDS[version]: schema_text(version) for version in IDS})


@pytest.fixture
def valid_event() -> dict[str, object]:
    return {
        "event_id": str(uuid4()),
        "user_id": "user-001",
        "programme_id": "programme-001",
        "event_type": "play_start",
        "position_s": 0.0,
        "duration_s": 1800.0,
        "device": "smart_tv",
        "event_time": 1790000000000,
        "produced_at": 1790000000025,
        "session_id": "session-1",
    }


def written(event: dict[str, object], version: int, schema_id: int = 0) -> bytes:
    return encode(event, parsed_schema(version), schema_id or IDS[version])


def test_v1_event_passes(valid_event: dict[str, object]) -> None:
    result = validate_event(written(valid_event, 1), SCHEMAS)

    assert result.event is not None
    assert result.dead_letter is None
    assert str(result.event.event_id) == valid_event["event_id"]
    assert result.event.session_id is None


def test_v2_event_is_read_as_v1(valid_event: dict[str, object]) -> None:
    result = validate_event(written(valid_event, 2), SCHEMAS)

    assert result.event is not None
    # The jobs read v1, which has no session_id, so Avro drops it.
    assert result.event.session_id is None
    assert result.event.programme_id == valid_event["programme_id"]


def test_a_v2_reader_sees_session_id(valid_event: dict[str, object]) -> None:
    v2_reader = parsed_schema(2)

    from_v2 = validate_event(written(valid_event, 2), SCHEMAS, v2_reader)
    from_v1 = validate_event(written(valid_event, 1), SCHEMAS, v2_reader)

    assert from_v2.event is not None and from_v2.event.session_id == "session-1"
    # Written before v2, so it has the default.
    assert from_v1.event is not None and from_v1.event.session_id is None


def test_a_broken_rule_is_dead_lettered_with_the_event(
    valid_event: dict[str, object],
) -> None:
    # Avro takes an empty string; the event's rules do not.
    valid_event["user_id"] = ""
    raw = written(valid_event, 2)

    result = validate_event(raw, SCHEMAS)

    assert result.event is None
    assert result.dead_letter is not None
    assert result.dead_letter["error_type"] == "schema_violation"
    assert "user_id" in result.dead_letter["reason"]
    assert result.dead_letter["schema_id"] == IDS[2]
    assert result.dead_letter["event"]["event_id"] == valid_event["event_id"]
    assert base64.b64decode(result.dead_letter["raw_base64"]) == raw


def test_json_is_dead_lettered_as_not_avro(valid_event: dict[str, object]) -> None:
    valid_event["event_time"] = "not-a-timestamp"
    raw = json.dumps(valid_event).encode()

    result = validate_event(raw, SCHEMAS)

    assert result.dead_letter is not None
    assert result.dead_letter["error_type"] == "not_avro"
    assert result.dead_letter["event"] is None
    assert base64.b64decode(result.dead_letter["raw_base64"]) == raw


@pytest.mark.parametrize("raw", [b"", b"\x00\x00\x00", b"\x01\x00\x00\x00\x0b"])
def test_bytes_without_the_header_are_not_avro(raw: bytes) -> None:
    result = validate_event(raw, SCHEMAS)

    assert result.dead_letter is not None
    assert result.dead_letter["error_type"] == "not_avro"


def test_an_unknown_schema_id_is_dead_lettered(valid_event: dict[str, object]) -> None:
    result = validate_event(written(valid_event, 2, schema_id=99), SCHEMAS)

    assert result.dead_letter is not None
    assert result.dead_letter["error_type"] == "unknown_schema"
    assert result.dead_letter["schema_id"] == 99


@pytest.mark.parametrize("cut", [6, 20])
def test_bytes_that_do_not_decode_are_invalid_avro(
    valid_event: dict[str, object], cut: int
) -> None:
    result = validate_event(written(valid_event, 2)[:cut], SCHEMAS)

    assert result.dead_letter is not None
    assert result.dead_letter["error_type"] == "invalid_avro"


def test_bytes_written_with_another_schema_than_their_id_are_invalid_avro(
    valid_event: dict[str, object],
) -> None:
    # Written as v2 but labelled v1: session_id is left over.
    raw = encode(valid_event, parsed_schema(2), IDS[1])

    result = validate_event(raw, SCHEMAS)

    assert result.dead_letter is not None
    assert result.dead_letter["error_type"] == "invalid_avro"
    assert "left over" in result.dead_letter["reason"]


def test_an_unreachable_registry_raises_instead_of_dead_lettering(
    valid_event: dict[str, object],
) -> None:
    def unreachable(schema_id: int) -> str:
        raise RegistryUnavailable("connection refused")

    # Flink restarts the job and tries again; the event is not lost.
    with pytest.raises(RegistryUnavailable):
        validate_event(written(valid_event, 2), WriterSchemas(unreachable))


def test_dead_letter_is_json_serialisable(valid_event: dict[str, object]) -> None:
    valid_event["device"] = ""

    result = validate_event(written(valid_event, 1), SCHEMAS)

    assert result.dead_letter is not None
    assert json.loads(json.dumps(result.dead_letter)) == result.dead_letter


def test_the_kafka_charset_gives_back_every_byte() -> None:
    every_byte = bytes(range(256))

    assert every_byte.decode(KAFKA_VALUE_CHARSET).encode(KAFKA_VALUE_CHARSET) == (
        every_byte
    )
