"""Publish synthetic viewing events to Kafka."""

from __future__ import annotations

import argparse
import json
import logging
import signal
import time
from collections.abc import Callable
from typing import Any

from confluent_kafka import KafkaError, KafkaException, Producer

from realtime_recommendation_pipeline.producer.generate import generate_events

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
) -> None:
    """Count successful and failed deliveries."""
    if error is not None:
        stats["failures"] += 1
        logger.error(
            "Delivery failed for topic=%s key=%s: %s",
            message.topic(),
            message.key(),
            error,
        )
        return

    stats["delivered"] += 1


def produce_events(
    *,
    bootstrap_servers: str,
    rate: float,
    duration: float,
    seed: int = 42,
    producer_factory: Callable[..., Producer] = Producer,
    malformed_rate: float = 0.0,
) -> dict[str, int]:
    """Generate and publish events at the requested rate."""
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

    event_count = int(rate * duration)
    events = generate_events(event_count, seed=seed, malformed_rate=malformed_rate)

    start_time = time.monotonic()
    next_event_time = start_time

    try:
        for event in events:
            if _STOP_REQUESTED:
                logger.info("Stop requested; ending event production.")
                break

            # Pace events using monotonic time to avoid clock adjustments.
            now = time.monotonic()
            delay = next_event_time - now

            if delay > 0:
                time.sleep(delay)

            if _STOP_REQUESTED:
                break

            user_id = str(event["user_id"])

            try:
                producer.produce(
                    topic="viewing-events",
                    key=user_id.encode("utf-8"),
                    value=json.dumps(event).encode("utf-8"),
                    callback=lambda error, message: _delivery_callback(
                        error,
                        message,
                        stats=stats,
                    ),
                )
            except BufferError:
                # The local producer queue is full. Poll to make progress,
                # then retry this event.
                producer.poll(0.1)

                producer.produce(
                    topic="viewing-events",
                    key=user_id.encode("utf-8"),
                    value=json.dumps(event).encode("utf-8"),
                    callback=lambda error, message: _delivery_callback(
                        error,
                        message,
                        stats=stats,
                    ),
                )

            # poll() serves delivery callbacks.
            producer.poll(0)

            next_event_time += 1.0 / rate

    except KafkaException:
        logger.exception("Kafka producer error")
        raise

    finally:
        logger.info("Flushing outstanding Kafka messages.")
        remaining = producer.flush(timeout=30)

        if remaining:
            stats["failures"] += remaining
            logger.error(
                "%d message(s) remained undelivered after flush.",
                remaining,
            )

    logger.info(
        "Production finished: delivered=%d failures=%d",
        stats["delivered"],
        stats["failures"],
    )

    return stats


def main() -> None:
    """Parse CLI arguments and start the producer."""
    parser = argparse.ArgumentParser(
        description="Publish synthetic viewing events to Kafka."
    )
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--rate", type=float, default=100.0)
    parser.add_argument("--duration", type=float, default=60.0)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--malformed-rate", type=float, default=0.0)

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    signal.signal(signal.SIGINT, _request_stop)
    signal.signal(signal.SIGTERM, _request_stop)

    produce_events(
        bootstrap_servers=args.bootstrap_servers,
        rate=args.rate,
        duration=args.duration,
        seed=args.seed,
        malformed_rate=args.malformed_rate,
    )


if __name__ == "__main__":
    main()
