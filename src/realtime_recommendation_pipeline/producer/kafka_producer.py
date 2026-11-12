"""Publish synthetic viewing events to Kafka, in Avro.

The producer registers the schema version it writes (the registry refuses one
that breaks an earlier version) and writes each event in the registry's wire
format (common/event_schemas.py).
"""

from __future__ import annotations

import argparse
import json
import logging
import signal
import time
from collections.abc import Callable, Sequence
from functools import partial
from typing import Any

from confluent_kafka import KafkaError, KafkaException, Producer

from realtime_recommendation_pipeline.common.event_schemas import (
    LATEST_VERSION,
    encode,
    parsed_schema,
    schema_text,
    subject,
    versions,
)
from realtime_recommendation_pipeline.common.logs import configure_logging
from realtime_recommendation_pipeline.common.schema_registry import SchemaRegistry
from realtime_recommendation_pipeline.producer.generate import generate_events
from realtime_recommendation_pipeline.settings import (
    KafkaSettings,
    LogSettings,
    SchemaRegistrySettings,
    load,
)

logger = logging.getLogger(__name__)

_STOP_REQUESTED = False


def _request_stop(signum: int, frame: Any) -> None:
    """Request a graceful stop after the current event."""
    global _STOP_REQUESTED
    _STOP_REQUESTED = True


def _delivery_callback(
    error: KafkaError | None,
    message: Any,
    *,
    stats: dict[str, int],
    event_id: object,
    event_time: object,
) -> None:
    """Count deliveries, and log each one by event_id for tracing."""
    if error is not None:
        stats["failures"] += 1
        logger.error(
            "event delivery failed",
            extra={"event_id": event_id, "topic": message.topic(), "error": str(error)},
        )
        return

    stats["delivered"] += 1
    # One line per event is too many for INFO; LOG_LEVEL=DEBUG turns them on.
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(
            "event delivered",
            extra={
                "event_id": event_id,
                "event_time": event_time,
                "topic": message.topic(),
                "partition": message.partition(),
                "offset": message.offset(),
            },
        )


def kafka_timestamp_ms(event: dict[str, object]) -> int:
    """Kafka record timestamp for an event: its event_time.

    Flink's Kafka source uses record timestamps as event time, so stamping
    records with event_time makes windows follow when people watched.
    Malformed events fall back to produced_at, which sits right next to the
    original event_time, so they cannot push a partition's watermark ahead.
    """
    event_time = event["event_time"]
    if isinstance(event_time, int):
        return event_time

    produced_at = event["produced_at"]
    if isinstance(produced_at, int):
        return produced_at

    raise ValueError("event has neither an integer event_time nor produced_at")


def live_event(event: dict[str, object], now_ms: int) -> dict[str, object]:
    """The event re-stamped as happening now, so windows follow the wall clock.

    Generated event times start on a fixed date and run hundreds of times
    faster than real time; a live demo needs them to be the time of sending.
    A malformed event_time is kept, so the event is still dead-lettered.
    """
    stamped = {**event, "produced_at": now_ms}
    if isinstance(event["event_time"], int):
        stamped["event_time"] = now_ms

    return stamped


def registered_schemas(
    registry: SchemaRegistry, schema_versions: Sequence[int]
) -> list[tuple[Any, int]]:
    """Each version's parsed schema and registry id, registering it if new."""
    schemas = []
    for version in schema_versions:
        schema_id = registry.register(subject(), schema_text(version))
        logger.info(
            "schema registered",
            extra={"subject": subject(), "version": version, "schema_id": schema_id},
        )
        schemas.append((parsed_schema(version), schema_id))
    return schemas


def event_bytes(event: dict[str, object], schema: Any, schema_id: int) -> bytes:
    """The event in Avro, or as JSON when Avro cannot hold it.

    A malformed event (make produce MALFORMED_RATE=...) has a field of the
    wrong type, which the schema cannot encode, so it goes as the JSON it is,
    the way a producer without the schema would send it. The job dead-letters
    it as not_avro.
    """
    try:
        return encode(event, schema, schema_id)
    except (TypeError, ValueError):
        return json.dumps(event).encode("utf-8")


def produce_events(
    *,
    bootstrap_servers: str,
    rate: float,
    duration: float,
    seed: int = 42,
    producer_factory: Callable[..., Producer] = Producer,
    malformed_rate: float = 0.0,
    dup_rate: float = 0.0,
    topic: str,
    registry: SchemaRegistry,
    schema_versions: Sequence[int] = (LATEST_VERSION,),
    live: bool = False,
) -> dict[str, int]:
    """Generate and publish events at the requested rate.

    The events are shared equally between schema_versions, in order: (1, 2)
    writes the first half as v1 and the rest as v2, as producers moving to v2
    part-way through would.
    """
    if rate <= 0:
        raise ValueError("rate must be greater than zero")

    if duration <= 0:
        raise ValueError("duration must be greater than zero")

    global _STOP_REQUESTED
    _STOP_REQUESTED = False

    producer = producer_factory(
        {
            "bootstrap.servers": bootstrap_servers,
            "enable.idempotence": True,
            "acks": "all",
        }
    )

    stats = {"delivered": 0, "failures": 0}
    schemas = registered_schemas(registry, schema_versions)

    event_count = int(rate * duration)
    events = generate_events(
        event_count, seed=seed, dup_rate=dup_rate, malformed_rate=malformed_rate
    )

    start_time = time.monotonic()
    next_event_time = start_time
    # When sending began and ended (epoch ms), for make bench: at high rates
    # generating the events above takes tens of seconds.
    stats["sent_from_ms"] = time.time_ns() // 1_000_000

    try:
        for index, event in enumerate(events):
            if _STOP_REQUESTED:
                logger.info("stop requested")
                break

            # Pace events using monotonic time to avoid clock adjustments.
            now = time.monotonic()
            delay = next_event_time - now

            if delay > 0:
                time.sleep(delay)

            if _STOP_REQUESTED:
                break

            if live:
                event = live_event(event, time.time_ns() // 1_000_000)

            user_id = str(event["user_id"])
            timestamp = kafka_timestamp_ms(event)
            schema, schema_id = schemas[index * len(schemas) // len(events)]
            value = event_bytes(event, schema, schema_id)
            on_delivery = partial(
                _delivery_callback,
                stats=stats,
                event_id=event["event_id"],
                event_time=event["event_time"],
            )

            try:
                producer.produce(
                    topic=topic,
                    key=user_id.encode("utf-8"),
                    value=value,
                    timestamp=timestamp,
                    callback=on_delivery,
                )
            except BufferError:
                # The local producer queue is full. Poll to make progress,
                # then retry this event.
                producer.poll(0.1)

                producer.produce(
                    topic=topic,
                    key=user_id.encode("utf-8"),
                    value=value,
                    timestamp=timestamp,
                    callback=on_delivery,
                )

            # poll() serves delivery callbacks.
            producer.poll(0)

            next_event_time += 1.0 / rate

    except KafkaException:
        logger.exception("Kafka producer error")
        raise

    finally:
        remaining = producer.flush(timeout=30)
        stats["sent_to_ms"] = time.time_ns() // 1_000_000

        if remaining:
            stats["failures"] += remaining
            logger.error("undelivered after flush", extra={"undelivered": remaining})

    logger.info(
        "production finished",
        extra={
            "topic": topic,
            "delivered": stats["delivered"],
            "failures": stats["failures"],
        },
    )

    return stats


def main() -> None:
    """Parse CLI arguments and start the producer."""
    parser = argparse.ArgumentParser(
        description="Publish synthetic viewing events to Kafka."
    )
    parser.add_argument("--rate", type=float, default=100.0)
    parser.add_argument("--duration", type=float, default=60.0)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--malformed-rate", type=float, default=0.0)
    parser.add_argument("--dup-rate", type=float, default=0.0)
    parser.add_argument(
        "--schema-version",
        type=int,
        choices=versions(),
        default=LATEST_VERSION,
        help="the event schema version to write",
    )
    parser.add_argument(
        "--live",
        action="store_true",
        help="stamp each event with the time it is sent instead of its generated time",
    )

    args = parser.parse_args()

    kafka = load(KafkaSettings)
    registry = SchemaRegistry(load(SchemaRegistrySettings).url)
    configure_logging("producer", load(LogSettings).log_level)

    signal.signal(signal.SIGINT, _request_stop)
    signal.signal(signal.SIGTERM, _request_stop)

    produce_events(
        bootstrap_servers=kafka.bootstrap_servers,
        rate=args.rate,
        duration=args.duration,
        seed=args.seed,
        malformed_rate=args.malformed_rate,
        dup_rate=args.dup_rate,
        topic=kafka.events_topic,
        registry=registry,
        schema_versions=(args.schema_version,),
        live=args.live,
    )


if __name__ == "__main__":
    main()
