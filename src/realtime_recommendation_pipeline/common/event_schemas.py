"""The viewing event's Avro schemas, and the bytes an event is on Kafka.

The schemas are schemas/viewing_event/v1.avsc, v2.avsc and so on: one file per
version, never changed once released, because events written with it stay in
the topic (docs/adr/0006-avro-events.md). Each event on the events topic is in
the Schema Registry's wire format: a zero byte, the registry's 4-byte id for
the schema the producer wrote it with, then the event in Avro's binary
encoding. A consumer decodes it with that writer schema and its own reader
schema, and Avro's schema resolution drops the fields the reader does not
know and fills in the defaults of those the writer did not have.
"""

from __future__ import annotations

import io
import json
import re
import struct
from collections.abc import Callable, Mapping
from functools import cache
from importlib.resources import files
from typing import Any

import fastavro
from fastavro.read import SchemaResolutionError

SCHEMA_DIR = files("realtime_recommendation_pipeline") / "schemas" / "viewing_event"
VERSION_FILE = re.compile(r"v([1-9][0-9]*)\.avsc")

MAGIC_BYTE = 0
# The magic byte, then the schema id as a 4-byte big-endian integer.
HEADER = struct.Struct(">bI")

# What fastavro raises for bytes that are not an event in the writer schema,
# or schemas that cannot read one another.
DECODE_ERRORS = (ValueError, TypeError, EOFError, LookupError, SchemaResolutionError)


class NotAvro(ValueError):
    """The bytes are not in the Schema Registry's wire format."""


def versions() -> list[int]:
    """The released versions, oldest first."""
    found = (VERSION_FILE.fullmatch(path.name) for path in SCHEMA_DIR.iterdir())
    return sorted(int(match.group(1)) for match in found if match)


@cache
def schema_text(version: int) -> str:
    return (SCHEMA_DIR / f"v{version}.avsc").read_text()


@cache
def parsed_schema(version: int) -> Any:
    return fastavro.parse_schema(json.loads(schema_text(version)))


LATEST_VERSION = versions()[-1]


def subject() -> str:
    """The registry subject for every version: the record's full name.

    One subject whatever the topic, so the demo, chaos and benchmark topics
    share the schemas of the events topic (RecordNameStrategy).
    """
    schema = json.loads(schema_text(LATEST_VERSION))
    return f"{schema['namespace']}.{schema['name']}"


def encode(event: Mapping[str, Any], schema: Any, schema_id: int) -> bytes:
    """The event in the wire format. Raises if the schema cannot hold it."""
    buffer = io.BytesIO()
    buffer.write(HEADER.pack(MAGIC_BYTE, schema_id))
    fastavro.schemaless_writer(buffer, schema, dict(event))
    return buffer.getvalue()


def schema_id_of(data: bytes) -> int:
    """The id of the schema the bytes were written with."""
    if len(data) < HEADER.size or data[0] != MAGIC_BYTE:
        raise NotAvro("does not start with a zero byte and a schema id")
    schema_id: int = HEADER.unpack_from(data)[1]
    return schema_id


def decode(data: bytes, writer: Any, reader: Any) -> dict[str, Any]:
    """The event as the reader schema describes it.

    Raises if the bytes do not hold one whole event in the writer schema.
    """
    body = io.BytesIO(data[HEADER.size :])
    record = fastavro.schemaless_reader(body, writer, reader)
    if body.read(1):
        raise ValueError("bytes left over after the event")
    if not isinstance(record, dict):
        raise TypeError("not a record")
    return record


class WriterSchemas:
    """Writer schemas by registry id, each fetched once and then kept."""

    def __init__(self, fetch: Callable[[int], str]) -> None:
        self.fetch = fetch
        self.parsed: dict[int, Any] = {}

    def get(self, schema_id: int) -> Any:
        if schema_id not in self.parsed:
            text = self.fetch(schema_id)
            self.parsed[schema_id] = fastavro.parse_schema(json.loads(text))
        return self.parsed[schema_id]
