import json

import pytest

from realtime_recommendation_pipeline.common.event_schemas import (
    LATEST_VERSION,
    NotAvro,
    WriterSchemas,
    decode,
    encode,
    parsed_schema,
    schema_id_of,
    schema_text,
    subject,
    versions,
)
from realtime_recommendation_pipeline.common.events import ViewingEvent
from realtime_recommendation_pipeline.producer.generate import generate_events

EVENTS = generate_events(200, seed=5)


def fields(version: int) -> dict[str, dict[str, object]]:
    return {
        field["name"]: field for field in json.loads(schema_text(version))["fields"]
    }


def test_versions_count_up_from_1_without_gaps() -> None:
    assert versions() == list(range(1, LATEST_VERSION + 1))
    assert LATEST_VERSION >= 2


def test_every_version_is_the_same_record() -> None:
    for version in versions():
        schema = json.loads(schema_text(version))
        assert f"{schema['namespace']}.{schema['name']}" == subject()


def test_v2_adds_only_an_optional_session_id() -> None:
    added = fields(2).keys() - fields(1).keys()

    assert added == {"session_id"}
    assert fields(2)["session_id"]["type"] == ["null", "string"]
    assert fields(2)["session_id"]["default"] is None
    assert fields(1).keys() <= fields(2).keys()


def test_the_model_has_the_newest_schemas_fields() -> None:
    assert set(ViewingEvent.model_fields) == set(fields(LATEST_VERSION))


@pytest.mark.parametrize("version", versions())
def test_generated_events_survive_the_round_trip(version: int) -> None:
    schema = parsed_schema(version)

    for event in EVENTS:
        decoded = decode(encode(event, schema, 3), schema, schema)
        expected = {name: event[name] for name in fields(version)}
        assert {**decoded, "event_id": str(decoded["event_id"])} == expected


def test_the_header_is_a_zero_byte_and_the_schema_id() -> None:
    data = encode(EVENTS[0], parsed_schema(1), 258)

    assert data[:5] == b"\x00\x00\x00\x01\x02"
    assert schema_id_of(data) == 258


@pytest.mark.parametrize("data", [b"", b"\x00\x00", b"{}", b"\x01\x00\x00\x00\x01x"])
def test_bytes_without_the_header_are_not_avro(data: bytes) -> None:
    with pytest.raises(NotAvro):
        schema_id_of(data)


def test_bytes_left_over_after_the_event_are_an_error() -> None:
    data = encode(EVENTS[0], parsed_schema(1), 1) + b"\x00"

    with pytest.raises(ValueError, match="left over"):
        decode(data, parsed_schema(1), parsed_schema(1))


def test_writer_schemas_are_fetched_once_each() -> None:
    fetched: list[int] = []

    def fetch(schema_id: int) -> str:
        fetched.append(schema_id)
        return schema_text(schema_id)

    schemas = WriterSchemas(fetch)
    for schema_id in [1, 2, 1, 2, 2]:
        schemas.get(schema_id)

    assert fetched == [1, 2]
