import json
from typing import Any

from realtime_recommendation_pipeline.producer.kafka_producer import (
    kafka_timestamp_ms,
    produce_events,
)


class FakeProducer:
    """Records produced values instead of sending them to Kafka."""

    def __init__(self) -> None:
        self.values: list[bytes] = []
        self.timestamps: list[int] = []

    def produce(
        self, *, topic: str, key: bytes, value: bytes, timestamp: int, callback: Any
    ) -> None:
        self.values.append(value)
        self.timestamps.append(timestamp)
        callback(None, None)

    def poll(self, timeout: float) -> int:
        return 0

    def flush(self, timeout: float) -> int:
        return 0


def produce_with_fake(**kwargs: Any) -> list[dict[str, Any]]:
    fake = FakeProducer()

    produce_events(
        bootstrap_servers="unused:9092",
        rate=10_000,
        duration=0.1,
        producer_factory=lambda _config: fake,
        **kwargs,
    )

    return [json.loads(value) for value in fake.values]


def test_no_malformed_events_by_default() -> None:
    events = produce_with_fake()

    assert len(events) == 1_000
    assert all(isinstance(event["event_time"], int) for event in events)


def test_malformed_rate_injects_malformed_events() -> None:
    events = produce_with_fake(malformed_rate=0.1)

    malformed = [event for event in events if event["event_time"] == "not-a-timestamp"]

    assert len(events) == 1_000
    assert 50 <= len(malformed) <= 150


def test_dup_rate_repeats_event_ids() -> None:
    events = produce_with_fake(dup_rate=0.1)

    event_ids = [event["event_id"] for event in events]

    assert len(set(event_ids)) == 1_000
    assert len(event_ids) > 1_000


def test_records_are_stamped_with_event_time() -> None:
    fake = FakeProducer()

    produce_events(
        bootstrap_servers="unused:9092",
        rate=10_000,
        duration=0.1,
        producer_factory=lambda _config: fake,
    )

    events = [json.loads(value) for value in fake.values]

    assert fake.timestamps == [event["event_time"] for event in events]


def test_malformed_event_time_falls_back_to_produced_at() -> None:
    event: dict[str, object] = {
        "event_time": "not-a-timestamp",
        "produced_at": 1_790_000_000_025,
    }

    assert kafka_timestamp_ms(event) == 1_790_000_000_025
