"""The results writer against real Kafka transactions and DynamoDB Local."""

import json
import time
import uuid
from collections.abc import Callable, Iterator
from typing import Any

import pytest
from botocore.exceptions import ClientError
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from testcontainers.community.kafka import KafkaContainer

from realtime_recommendation_pipeline.common.kafka import downstream_consumer_config
from realtime_recommendation_pipeline.common.trending_table import plain
from realtime_recommendation_pipeline.writer.results_writer import (
    KafkaDeadLetters,
    ResultsWriter,
    writer_consumer_config,
)
from realtime_recommendation_pipeline.writer.retry import RetryPolicy

pytestmark = pytest.mark.integration

EMITTED_AT_MS = int(time.time() * 1000)


@pytest.fixture(scope="module")
def kafka() -> Iterator[str]:
    container = (
        KafkaContainer("confluentinc/cp-kafka:7.7.0")
        # One broker, so the transaction log cannot ask for three replicas.
        .with_env("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
        .with_env("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
    )
    with container:
        yield container.get_bootstrap_server()


def create_topics(bootstrap_servers: str, *topics: str) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    futures = admin.create_topics([NewTopic(topic, 1, 1) for topic in topics])
    for future in futures.values():
        future.result(30)


def ranked_rows(window_end: str, scores: list[int]) -> list[dict[str, Any]]:
    return [
        {
            "window_start": "2026-09-21T13:00:00Z",
            "window_end": window_end,
            "rank": rank,
            "programme_id": f"programme-{rank:04d}",
            "score": score,
            "max_produced_at": 1790000000000,
            "emitted_at": EMITTED_AT_MS,
        }
        for rank, score in enumerate(scores, start=1)
    ]


def publish(
    producer: Producer, topic: str, rows: list[dict[str, Any]], *, commit: bool
) -> None:
    """Write rows in one transaction, as Flink does for each checkpoint."""
    producer.begin_transaction()
    for row in rows:
        producer.produce(topic, json.dumps(row).encode())
    if commit:
        producer.commit_transaction(30)
    else:
        producer.abort_transaction(30)


def flink_like_producer(bootstrap_servers: str) -> Producer:
    producer = Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "transactional.id": f"flink-like-{uuid.uuid4()}",
        }
    )
    producer.init_transactions(30)
    return producer


def start_writer(
    bootstrap_servers: str, topic: str, group_id: str, table: Any, dlq_topic: str
) -> tuple[ResultsWriter, Consumer]:
    # Same group and instance id on every start, like a restarted process.
    consumer = Consumer(
        writer_consumer_config(bootstrap_servers, group_id, f"{group_id}-1")
    )
    producer = Producer({"bootstrap.servers": bootstrap_servers, "acks": "all"})
    writer = ResultsWriter(
        consumer,
        table,
        KafkaDeadLetters(producer, dlq_topic),
        RetryPolicy(max_attempts=3, base_delay_s=0.01),
        idle_flush_s=1.0,
    )
    consumer.subscribe([topic], on_revoke=lambda _c, _p: writer.buffer.clear())
    return writer, consumer


def poll_until(writer: ResultsWriter, done: Callable[[], bool]) -> None:
    deadline = time.monotonic() + 60
    while not done():
        assert time.monotonic() < deadline, "writer did not get there in 60 s"
        writer.poll_once(0.2)


def ranked(item: dict[str, Any]) -> list[tuple[int, str, int]]:
    return [
        (entry["rank"], entry["programme_id"], entry["score"])
        for entry in plain(item)["items"]
    ]


def test_writer_killed_mid_window_restarts_with_correct_items(
    kafka: str, dynamodb: Any
) -> None:
    topic, dlq_topic = f"trending-{uuid.uuid4()}", f"dlq-{uuid.uuid4()}"
    group_id = f"results-writer-{uuid.uuid4()}"
    create_topics(kafka, topic, dlq_topic)
    table = dynamodb.Table("trending")

    flink = flink_like_producer(kafka)
    publish(flink, topic, ranked_rows("2026-09-21T14:00:00Z", [9, 5, 2]), commit=True)
    # A failed checkpoint: read_committed must never show these rows.
    publish(flink, topic, ranked_rows("2026-09-21T14:05:00Z", [999, 998]), commit=False)
    publish(flink, topic, ranked_rows("2026-09-21T14:05:00Z", [7, 3, 1]), commit=True)
    publish(flink, topic, ranked_rows("2026-09-21T14:10:00Z", [8, 6]), commit=True)

    def has(window_end: str) -> Callable[[], bool]:
        def check() -> bool:
            key = {"pk": "TRENDING#1h", "sk": window_end}
            return "Item" in table.get_item(Key=key)

        return check

    # The first writer stops as soon as 14:00 is written. It has read one row
    # of 14:05 by then (that row is what completed 14:00), so it stops halfway
    # through a window, and nothing of 14:05 has been committed.
    writer, consumer = start_writer(kafka, topic, group_id, table, dlq_topic)
    poll_until(writer, has("2026-09-21T14:00:00Z"))
    assert writer.buffer.current is not None
    assert writer.buffer.current.window_end == "2026-09-21T14:05:00Z"
    consumer.close()

    writer, consumer = start_writer(kafka, topic, group_id, table, dlq_topic)
    poll_until(writer, has("2026-09-21T14:10:00Z"))
    consumer.close()

    items = table.query(
        KeyConditionExpression="pk = :pk",
        ExpressionAttributeValues={":pk": "TRENDING#1h"},
    )["Items"]
    assert {item["sk"]: ranked(item) for item in items} == {
        "2026-09-21T14:00:00Z": [
            (1, "programme-0001", 9),
            (2, "programme-0002", 5),
            (3, "programme-0003", 2),
        ],
        "2026-09-21T14:05:00Z": [
            (1, "programme-0001", 7),
            (2, "programme-0002", 3),
            (3, "programme-0003", 1),
        ],
        "2026-09-21T14:10:00Z": [(1, "programme-0001", 8), (2, "programme-0002", 6)],
    }


class AlwaysThrottled:
    def put_item(self, Item: dict[str, Any]) -> None:
        raise ClientError(
            {
                "Error": {"Code": "ThrottlingException", "Message": "slow down"},
                "ResponseMetadata": {"HTTPStatusCode": 400},
            },
            "PutItem",
        )


def test_window_that_cannot_be_written_goes_to_the_dlq(kafka: str) -> None:
    topic, dlq_topic = f"trending-{uuid.uuid4()}", f"dlq-{uuid.uuid4()}"
    group_id = f"results-writer-{uuid.uuid4()}"
    create_topics(kafka, topic, dlq_topic)

    flink = flink_like_producer(kafka)
    publish(flink, topic, ranked_rows("2026-09-21T14:00:00Z", [9, 5]), commit=True)

    writer, consumer = start_writer(
        kafka, topic, group_id, AlwaysThrottled(), dlq_topic
    )
    # Rows are at offsets 0 and 1 (2 holds the transaction's commit marker), so
    # a committed offset of 2 means the window was handled.
    poll_until(
        writer,
        lambda: consumer.committed([TopicPartition(topic, 0)], 10)[0].offset == 2,
    )
    consumer.close()

    dlq = Consumer(downstream_consumer_config(kafka, f"dlq-reader-{uuid.uuid4()}"))
    dlq.subscribe([dlq_topic])
    message = None
    deadline = time.monotonic() + 30
    while message is None and time.monotonic() < deadline:
        message = dlq.poll(1.0)
    dlq.close()

    assert message is not None
    record = json.loads(message.value())
    assert record["window_end"] == "2026-09-21T14:00:00Z"
    assert record["attempts"] == 3
    assert "ThrottlingException" in record["error"]
    assert [entry["score"] for entry in record["item"]["items"]] == [9, 5]
