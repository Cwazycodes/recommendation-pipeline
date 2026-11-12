import json
import time
from typing import Any

from realtime_recommendation_pipeline.common.event_schemas import (
    LATEST_VERSION,
    WriterSchemas,
    decode,
    parsed_schema,
    schema_id_of,
    schema_text,
    subject,
)
from realtime_recommendation_pipeline.producer.kafka_producer import (
    event_bytes,
    kafka_timestamp_ms,
    live_event,
    produce_events,
)


class FakeProducer:
    """Records produced values instead of sending them to Kafka."""

    def __init__(self) -> None:
        self.topics: list[str] = []
        self.values: list[bytes] = []
        self.timestamps: list[int] = []

    def produce(
        self, *, topic: str, key: bytes, value: bytes, timestamp: int, callback: Any
    ) -> None:
        self.topics.append(topic)
        self.values.append(value)
        self.timestamps.append(timestamp)
        callback(None, None)

    def poll(self, timeout: float) -> int:
        return 0

    def flush(self, timeout: float) -> int:
        return 0


class FakeRegistry:
    """Gives each new schema the next id, as the Schema Registry does."""

    def __init__(self) -> None:
        self.ids: dict[str, int] = {}
        self.subjects: list[str] = []

    def register(self, subject: str, schema: str) -> int:
        self.subjects.append(subject)
        return self.ids.setdefault(schema, len(self.ids) + 1)

    def schema(self, schema_id: int) -> str:
        return next(text for text, i in self.ids.items() if i == schema_id)


def produce_with_fake(**kwargs: Any) -> tuple[FakeProducer, FakeRegistry]:
    fake, registry = FakeProducer(), FakeRegistry()
    options = {"rate": 10_000, "duration": 0.1, "topic": "viewing-events", **kwargs}

    produce_events(
        bootstrap_servers="unused:9092",
        producer_factory=lambda _config: fake,
        registry=registry,  # type: ignore[arg-type]
        **options,
    )

    return fake, registry


def read(fake: FakeProducer, registry: FakeRegistry) -> list[dict[str, Any]]:
    """Each value as the newest schema reads it, or as JSON if it is not Avro."""
    schemas = WriterSchemas(registry.schema)
    reader = parsed_schema(LATEST_VERSION)
    events = []
    for value in fake.values:
        if value.startswith(b"{"):
            events.append(json.loads(value))
            continue
        event = decode(value, schemas.get(schema_id_of(value)), reader)
        events.append({**event, "event_id": str(event["event_id"])})
    return events


def test_no_malformed_events_by_default() -> None:
    fake, registry = produce_with_fake()
    events = read(fake, registry)

    assert len(events) == 1_000
    assert all(isinstance(event["event_time"], int) for event in events)


def test_events_are_avro_in_the_newest_schema() -> None:
    fake, registry = produce_with_fake()

    newest = registry.ids[schema_text(LATEST_VERSION)]
    assert {schema_id_of(value) for value in fake.values} == {newest}
    assert registry.subjects == [subject()]


def test_malformed_rate_injects_malformed_events() -> None:
    fake, registry = produce_with_fake(malformed_rate=0.1)
    events = read(fake, registry)

    malformed = [event for event in events if event["event_time"] == "not-a-timestamp"]

    assert len(events) == 1_000
    assert 50 <= len(malformed) <= 150
    # Avro cannot hold them, so they go as JSON.
    assert sum(value.startswith(b"{") for value in fake.values) == len(malformed)


def test_dup_rate_repeats_event_ids() -> None:
    fake, registry = produce_with_fake(dup_rate=0.1)

    event_ids = [event["event_id"] for event in read(fake, registry)]

    assert len(set(event_ids)) == 1_000
    assert len(event_ids) > 1_000


def test_events_are_shared_between_versions_in_order() -> None:
    fake, registry = produce_with_fake(schema_versions=(1, 2))
    events = read(fake, registry)

    v1, v2 = registry.ids[schema_text(1)], registry.ids[schema_text(2)]
    ids = [schema_id_of(value) for value in fake.values]
    assert ids == [v1] * 500 + [v2] * 500
    # A v1 event has no session_id, so the v2 reader gives it the default.
    assert all(event["session_id"] is None for event in events[:500])
    assert all(event["session_id"] for event in events[500:])


def test_an_event_avro_cannot_hold_goes_as_json() -> None:
    event: dict[str, object] = {"event_id": "e1", "event_time": "not-a-timestamp"}

    assert event_bytes(event, parsed_schema(1), 1) == json.dumps(event).encode()


def test_records_are_stamped_with_event_time() -> None:
    fake, registry = produce_with_fake()

    events = read(fake, registry)

    assert fake.timestamps == [event["event_time"] for event in events]


def test_malformed_event_time_falls_back_to_produced_at() -> None:
    event: dict[str, object] = {
        "event_time": "not-a-timestamp",
        "produced_at": 1_790_000_000_025,
    }

    assert kafka_timestamp_ms(event) == 1_790_000_000_025


def test_live_events_are_stamped_with_the_time_they_are_sent() -> None:
    before_ms = time.time_ns() // 1_000_000

    fake, registry = produce_with_fake(duration=0.01, live=True)

    after_ms = time.time_ns() // 1_000_000
    events = read(fake, registry)
    assert len(events) == 100
    for event, timestamp in zip(events, fake.timestamps, strict=True):
        assert event["event_time"] == event["produced_at"] == timestamp
        assert before_ms <= timestamp <= after_ms


def test_live_mode_keeps_a_malformed_event_time() -> None:
    event: dict[str, object] = {"event_time": "not-a-timestamp", "produced_at": 1}

    assert live_event(event, 2_000) == {
        "event_time": "not-a-timestamp",
        "produced_at": 2_000,
    }


def test_events_go_to_the_given_topic() -> None:
    fake, _ = produce_with_fake(duration=0.01, topic="viewing-events-demo")

    assert set(fake.topics) == {"viewing-events-demo"}


def test_stats_say_when_sending_began_and_ended() -> None:
    before_ms = time.time_ns() // 1_000_000

    stats = produce_events(
        bootstrap_servers="unused:9092",
        rate=10_000,
        duration=0.05,
        producer_factory=lambda _config: FakeProducer(),
        topic="viewing-events",
        registry=FakeRegistry(),  # type: ignore[arg-type]
    )

    after_ms = time.time_ns() // 1_000_000
    assert stats["delivered"] == 500
    assert before_ms <= stats["sent_from_ms"] <= stats["sent_to_ms"] <= after_ms
