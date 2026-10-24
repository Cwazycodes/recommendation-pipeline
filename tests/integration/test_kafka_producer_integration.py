"""Integration test for the Kafka producer."""

import json
import time
from uuid import uuid4

import pytest
from confluent_kafka import Consumer
from testcontainers.community.kafka import KafkaContainer

from realtime_recommendation_pipeline.producer.kafka_producer import produce_events


@pytest.mark.integration
def test_producer_publishes_and_consumer_reads_n_events() -> None:
    event_count = 20

    with KafkaContainer("confluentinc/cp-kafka:7.7.0") as kafka:
        bootstrap_servers = kafka.get_bootstrap_server()

        stats = produce_events(
            bootstrap_servers=bootstrap_servers,
            rate=float(event_count),
            duration=1.0,
            seed=123,
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
            consumer.subscribe(["viewing-events"])

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

        for message in messages:
            assert message.key() is not None

            event = json.loads(message.value().decode("utf-8"))
            assert event["user_id"] == message.key().decode("utf-8")
