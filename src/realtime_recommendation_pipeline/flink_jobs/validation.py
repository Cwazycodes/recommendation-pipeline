"""Validate raw viewing events against the RP-07 event contract."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any

from pydantic import ValidationError

from realtime_recommendation_pipeline.common.events import ViewingEvent

logger = logging.getLogger(__name__)


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


def build_dead_letter(raw: str, error_type: str, reason: str) -> dict[str, Any]:
    """Wrap a rejected payload with the reason it was rejected."""
    return {
        "error_type": error_type,
        "reason": reason,
        "raw": raw,
        "failed_at": time.time_ns() // 1_000_000,
    }


def validate_event(raw: str) -> ValidationResult:
    """Parse and validate one raw Kafka value without ever raising."""
    try:
        event = ViewingEvent.model_validate_json(raw)
    except ValidationError as exc:
        if any(detail["type"] == "json_invalid" for detail in exc.errors()):
            error_type = "invalid_json"
        else:
            error_type = "schema_violation"

        return ValidationResult(
            dead_letter=build_dead_letter(raw, error_type, describe_errors(exc))
        )
    except Exception as exc:
        # Last resort: one bad event must never crash the job.
        logger.exception("Unexpected error while validating an event")
        return ValidationResult(
            dead_letter=build_dead_letter(raw, "unexpected_error", repr(exc))
        )

    return ValidationResult(event=event)
