"""Integration test for the Kafka producer."""

import time
from uuid import uuid4

import pytest
from confluent_kafka import Consumer
from testcontainers.community.kafka import KafkaContainer

from realtime_recommendation_pipeline.common.event_schemas import WriterSchemas
from realtime_recommendation_pipeline.flink_jobs.validation import validate_event
from realtime_recommendation_pipeline.producer.kafka_producer import produce_events

TOPIC = "viewing-events"


class InMemoryRegistry:
    """Stands in for the Schema Registry, giving ids as it does."""

    def __init__(self) -> None:
        self.schemas: list[str] = []

    def register(self, subject: str, schema: str) -> int:
        if schema not in self.schemas:
            self.schemas.append(schema)
        return self.schemas.index(schema) + 1

    def schema(self, schema_id: int) -> str:
        return self.schemas[schema_id - 1]


@pytest.mark.integration
def test_producer_publishes_and_consumer_reads_n_events() -> None:
    event_count = 20

    with KafkaContainer("confluentinc/cp-kafka:7.7.0") as kafka:
        bootstrap_servers = kafka.get_bootstrap_server()
        registry = InMemoryRegistry()

        stats = produce_events(
            bootstrap_servers=bootstrap_servers,
            rate=float(event_count),
            duration=1.0,
            seed=123,
            topic=TOPIC,
            registry=registry,  # type: ignore[arg-type]
        )

        assert stats["failures"] == 0
        assert stats["delivered"] == event_count

        consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": f"rp10-test-{uuid4()}",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )

        messages = []

        try:
            consumer.subscribe([TOPIC])

            deadline = time.monotonic() + 15

            while len(messages) < event_count and time.monotonic() < deadline:
                message = consumer.poll(1.0)

                if message is None:
                    continue

                if message.error():
                    raise AssertionError(message.error())

                messages.append(message)

        finally:
            consumer.close()

        assert len(messages) == event_count

        # What the jobs do with each value: decode it as v1 and validate it.
        schemas = WriterSchemas(registry.schema)

        for message in messages:
            assert message.key() is not None

            result = validate_event(message.value(), schemas)
            assert result.event is not None
            assert result.event.user_id == message.key().decode("utf-8")
