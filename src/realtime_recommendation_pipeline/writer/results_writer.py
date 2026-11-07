"""Write each window on the trending topic to the DynamoDB trending table.

Effectively once: a window's offsets are committed only after its item is
written (or dead-lettered), so a crash replays at most the window being read,
and writing a window again replaces its item with the same data and a new
written_at. It serves Prometheus metrics on how late each window is written.
"""

from __future__ import annotations

import argparse
import json
import logging
import signal
import time
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from typing import Any

from botocore.config import Config
from botocore.exceptions import ConnectionError as BotoConnectionError
from botocore.exceptions import HTTPClientError
from confluent_kafka import (
    Consumer,
    KafkaError,
    KafkaException,
    Producer,
    TopicPartition,
)

from realtime_recommendation_pipeline.common.kafka import downstream_consumer_config
from realtime_recommendation_pipeline.common.logs import configure_logging
from realtime_recommendation_pipeline.common.trending_table import (
    PARTITION_KEY,
    TIME_FORMAT,
    dynamodb_resource,
    millis_text,
    put_window,
)
from realtime_recommendation_pipeline.settings import (
    DynamoDBSettings,
    KafkaSettings,
    LogSettings,
    MetricsSettings,
    load,
)
from realtime_recommendation_pipeline.writer.metrics import WriterMetrics
from realtime_recommendation_pipeline.writer.retry import (
    RetryPolicy,
    WriteFailure,
    write_with_retry,
)
from realtime_recommendation_pipeline.writer.windows import (
    Window,
    WindowBuffer,
    emitted_at,
    window_to_item,
)

logger = logging.getLogger(__name__)

# The writer retries with its own backoff and counts the tries, so boto3 must
# not retry underneath it.
NO_SDK_RETRIES = Config(retries={"total_max_attempts": 1})

# DynamoDB cannot be reached at all: refused or timed-out connections.
UNREACHABLE = (BotoConnectionError, HTTPClientError)

# While DynamoDB is unreachable the writer waits for as long as it takes: 1 s, 2 s, 4 s ... up to 30 s between tries, with jitter. max_attempts is not used here.
OUTAGE_BACKOFF = RetryPolicy(base_delay_s=1.0, max_delay_s=30.0)

# The writer does not poll Kafka while it waits out an outage. Heartbeats keep it in the group, but without a poll for max.poll.interval.ms (5 minutes by default) librdkafka leaves the group, and the next commit would fail.
MAX_POLL_INTERVAL_MS = 30 * 60 * 1000


def utc_now() -> datetime:
    return datetime.now(UTC)


def writer_consumer_config(
    bootstrap_servers: str, group_id: str, instance_id: str
) -> dict[str, str]:
    """read_committed, manual-commit consumer settings, as a static member.

    A killed writer never leaves its group, so a restarted one would wait for
    the dead one's session to time out (45 s) before getting the partition.
    With a fixed group.instance.id the restarted writer takes the dead one's
    place at once. A second writer with the same id fences off the first.
    """
    config = downstream_consumer_config(bootstrap_servers, group_id)
    config["group.instance.id"] = instance_id
    config["max.poll.interval.ms"] = str(MAX_POLL_INTERVAL_MS)

    return config


class KafkaDeadLetters:
    """Sends windows that could not be written to the writer DLQ topic."""

    def __init__(self, producer: Any, topic: str) -> None:
        self.producer = producer
        self.topic = topic

    def send(self, record: dict[str, Any]) -> None:
        """Send one record and wait for it; raises if Kafka did not take it."""
        errors: list[KafkaError] = []

        def on_delivery(error: KafkaError | None, _message: Any) -> None:
            if error is not None:
                errors.append(error)

        self.producer.produce(
            self.topic,
            key=record["window_end"].encode(),
            value=json.dumps(record).encode(),
            on_delivery=on_delivery,
        )
        if self.producer.flush(10) or errors:
            raise KafkaException(errors[0] if errors else "DLQ delivery timed out")


class ResultsWriter:
    """Reads ranked rows, writes one item per window, then commits."""

    def __init__(
        self,
        consumer: Any,
        table: Any,
        dead_letters: Any,
        retry: RetryPolicy | None = None,
        idle_flush_s: float = 2.0,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        partition_key: str = PARTITION_KEY,
        outage_backoff: RetryPolicy = OUTAGE_BACKOFF,
        now: Callable[[], datetime] = utc_now,
        metrics: WriterMetrics | None = None,
    ) -> None:
        self.consumer = consumer
        self.table = table
        self.dead_letters = dead_letters
        self.retry = retry or RetryPolicy()
        self.idle_flush_s = idle_flush_s
        self.clock = clock
        self.sleep = sleep
        self.partition_key = partition_key
        self.outage_backoff = outage_backoff
        self.now = now
        self.metrics = metrics or WriterMetrics()
        self.buffer = WindowBuffer()
        self.last_message_at = clock()

    def poll_once(self, timeout_s: float = 0.5) -> None:
        """Handle at most one message, flushing any window it completes."""
        message = self.consumer.poll(timeout_s)

        if message is None:
            # Nothing new: the buffered window has had time to arrive in full.
            if self.clock() - self.last_message_at >= self.idle_flush_s:
                window = self.buffer.take()
                if window is not None:
                    self.flush(window)
            return

        if message.error():
            raise KafkaException(message.error())

        self.last_message_at = self.clock()
        value = message.value()
        if value is None:
            return

        partition = (message.topic(), message.partition())
        complete = self.buffer.add(partition, message.offset(), json.loads(value))
        if complete is not None:
            self.flush(complete)

    def flush(self, window: Window) -> None:
        """Write the window's item, or dead-letter it, and only then commit."""
        item = window_to_item(window, self.partition_key)
        failure = self.write_when_reachable(item)

        if failure is None:
            window_end = datetime.fromisoformat(window.window_end)
            written_at = datetime.fromisoformat(item["written_at"])
            self.metrics.window_written(window_end, emitted_at(window), written_at)
            logger.info(
                "wrote window",
                extra={
                    "window_end": window.window_end,
                    "programmes": len(item["items"]),
                    "generated_at": item["generated_at"],
                    "written_at": item["written_at"],
                    "freshness_s": (written_at - window_end).total_seconds(),
                },
            )
        else:
            self.dead_letters.send(
                {
                    "window_end": window.window_end,
                    "item": item,
                    "error": failure.error,
                    "attempts": failure.attempts,
                    "dead_lettered_at": self.now().strftime(TIME_FORMAT),
                }
            )
            self.metrics.dead_lettered.inc()
            logger.error(
                "dead-lettered window",
                extra={
                    "window_end": window.window_end,
                    "attempts": failure.attempts,
                    "error": failure.error,
                },
            )

        self.commit()

    def write_when_reachable(self, item: dict[str, Any]) -> WriteFailure | None:
        """Write with retries, first waiting out any time DynamoDB is unreachable.

        Nothing is committed while it waits, so a crash during an outage loses
        nothing, and an outage never sends windows to the DLQ.
        """
        outage_tries = 0
        while True:
            try:
                return write_with_retry(
                    lambda: self.put(item), self.retry, sleep=self.sleep
                )
            except UNREACHABLE as exc:
                outage_tries += 1
                delay = self.outage_backoff.delay(outage_tries)
                logger.warning(
                    "dynamodb unreachable, not committing",
                    extra={
                        "window_end": item["sk"],
                        "next_try": outage_tries + 1,
                        "retry_in_s": round(delay, 1),
                        "error": str(exc),
                    },
                )
                self.sleep(delay)

    def put(self, item: dict[str, Any]) -> None:
        """One put_item, stamped with written_at: when this try is made.

        item gets written_at only once the put succeeds, so a dead-lettered
        item never says it was written.
        """
        written_at = millis_text(self.now())
        put_window(self.table, {**item, "written_at": written_at})
        item["written_at"] = written_at

    def commit(self) -> None:
        """Commit every offset before the window still being read."""
        offsets = [
            TopicPartition(topic, partition, offset)
            for (topic, partition), offset in self.buffer.committable_offsets().items()
        ]
        if offsets:
            self.consumer.commit(offsets=offsets, asynchronous=False)

    def run(self, should_stop: Callable[[], bool]) -> None:
        """Poll until should_stop() is true."""
        while not should_stop():
            self.poll_once()


def main(argv: Sequence[str] | None = None) -> None:
    """Hosts, topics, group and table come from the environment (settings.py)."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--idle-flush-seconds", type=float, default=2.0)
    parser.add_argument("--max-attempts", type=int, default=5)
    args = parser.parse_args(argv)

    kafka = load(KafkaSettings)
    dynamo = load(DynamoDBSettings)
    endpoint = load(MetricsSettings)
    configure_logging("results-writer", load(LogSettings).log_level)

    # Served from the start, so Prometheus sees the counters at zero first.
    metrics = WriterMetrics()
    server = metrics.serve(endpoint.writer_host, endpoint.writer_port)

    consumer = Consumer(
        writer_consumer_config(
            kafka.bootstrap_servers, kafka.writer_group_id, kafka.writer_instance_id
        )
    )
    producer = Producer(
        {
            "bootstrap.servers": kafka.bootstrap_servers,
            "enable.idempotence": True,
            "acks": "all",
        }
    )
    dynamodb = dynamodb_resource(dynamo.endpoint_url, config=NO_SDK_RETRIES)
    table = dynamodb.Table(dynamo.table)

    writer = ResultsWriter(
        consumer,
        table,
        KafkaDeadLetters(producer, kafka.writer_dlq_topic),
        RetryPolicy(max_attempts=args.max_attempts),
        idle_flush_s=args.idle_flush_seconds,
        partition_key=dynamo.partition_key,
        metrics=metrics,
    )

    # Rows buffered for a partition this writer loses were never committed, so
    # whichever consumer gets the partition next reads them again.
    consumer.subscribe(
        [kafka.trending_topic],
        on_revoke=lambda _consumer, _partitions: writer.buffer.clear(),
    )

    stop_requested = False

    def request_stop(signum: int, frame: Any) -> None:
        nonlocal stop_requested
        stop_requested = True

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    logger.info(
        "writer started",
        extra={
            "topic": kafka.trending_topic,
            "table": dynamo.table,
            "partition_key": dynamo.partition_key,
            "group_id": kafka.writer_group_id,
            "metrics_port": endpoint.writer_port,
        },
    )
    try:
        writer.run(lambda: stop_requested)
    finally:
        # No commit here: a window that was only partly read is read again
        # from its first row on the next start.
        consumer.close()
        server.shutdown()
        logger.info("writer stopped")


if __name__ == "__main__":
    main()
