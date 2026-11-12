"""Decode and validate raw viewing events: Avro first, then the event's rules.

Both jobs read every event as v1 of the schema, whichever version it was
written with: they use nothing a later version adds, and Avro drops what the
reader does not know. That makes them the v1 consumers a compatible change
must not break (docs/adr/0006-avro-events.md).
"""

from __future__ import annotations

import base64
import logging
import time
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from pydantic import ValidationError

from realtime_recommendation_pipeline.common.event_schemas import (
    DECODE_ERRORS,
    NotAvro,
    WriterSchemas,
    decode,
    parsed_schema,
    schema_id_of,
)
from realtime_recommendation_pipeline.common.events import ViewingEvent
from realtime_recommendation_pipeline.common.schema_registry import UnknownSchema

logger = logging.getLogger(__name__)

READER_VERSION = 1

# The Kafka source decodes each value with this charset. It maps every byte to
# one character, so value.encode(KAFKA_VALUE_CHARSET) gives back the exact
# bytes: PyFlink has no deserializer that hands Python the bytes themselves.
KAFKA_VALUE_CHARSET = "ISO-8859-1"


@dataclass(frozen=True)
class ValidationResult:
    """Outcome of validating one raw event: an event or a dead letter."""

    event: ViewingEvent | None = None
    dead_letter: dict[str, Any] | None = None


def describe_errors(exc: ValidationError) -> str:
    """Summarise Pydantic errors as 'field: message; field: message'."""
    parts = []

    for detail in exc.errors(include_url=False):
        location = ".".join(str(part) for part in detail["loc"]) or "<root>"
        parts.append(f"{location}: {detail['msg']}")

    return "; ".join(parts)


def build_dead_letter(
    raw: bytes,
    error_type: str,
    reason: str,
    schema_id: int | None = None,
    event: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Wrap a rejected payload with the reason it was rejected.

    The bytes are kept whole in base64; an event that decoded but broke a rule
    is also there as the reader saw it, event_id included.
    """
    return {
        "error_type": error_type,
        "reason": reason,
        "schema_id": schema_id,
        "event": None if event is None else json_safe(event),
        "raw_base64": base64.b64encode(raw).decode("ascii"),
        "failed_at": time.time_ns() // 1_000_000,
    }


def json_safe(record: dict[str, Any]) -> dict[str, Any]:
    return {
        name: str(value) if isinstance(value, UUID) else value
        for name, value in record.items()
    }


def validate_event(
    raw: bytes, writer_schemas: WriterSchemas, reader: Any = None
) -> ValidationResult:
    """Decode and validate one Kafka value.

    Bad bytes are dead-lettered, never raised, so one bad event cannot stop
    the job. The exception is the registry being unreachable while a new
    schema id is looked up: that raises, so Flink restarts and tries again,
    rather than dead-lettering events that are fine.
    """
    reader = parsed_schema(READER_VERSION) if reader is None else reader
    schema_id = None
    try:
        schema_id = schema_id_of(raw)
        record = decode(raw, writer_schemas.get(schema_id), reader)
    except NotAvro as exc:
        return ValidationResult(
            dead_letter=build_dead_letter(raw, "not_avro", str(exc))
        )
    except UnknownSchema as exc:
        return ValidationResult(
            dead_letter=build_dead_letter(raw, "unknown_schema", str(exc), schema_id)
        )
    except DECODE_ERRORS as exc:
        return ValidationResult(
            dead_letter=build_dead_letter(raw, "invalid_avro", repr(exc), schema_id)
        )

    try:
        event = ViewingEvent.model_validate(record)
    except ValidationError as exc:
        return ValidationResult(
            dead_letter=build_dead_letter(
                raw, "schema_violation", describe_errors(exc), schema_id, record
            )
        )
    except Exception as exc:
        # Last resort: one bad event must never crash the job.
        logger.exception("Unexpected error while validating an event")
        return ValidationResult(
            dead_letter=build_dead_letter(
                raw, "unexpected_error", repr(exc), schema_id, record
            )
        )

    return ValidationResult(event=event)
