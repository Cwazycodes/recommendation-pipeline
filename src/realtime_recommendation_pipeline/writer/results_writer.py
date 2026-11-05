"""Write each trending-results window to the DynamoDB 'trending' table.

Effectively once: a window's offsets are committed only after its item is
written (or dead-lettered), so a crash replays at most the window being read,
and writing a window again replaces its item with identical data.
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
from realtime_recommendation_pipeline.common.trending_table import (
    PARTITION_KEY,
    TIME_FORMAT,
    dynamodb_resource,
    put_window,
)
from realtime_recommendation_pipeline.writer.retry import (
    RetryPolicy,
    WriteFailure,
    write_with_retry,
)
from realtime_recommendation_pipeline.writer.windows import (
    Window,
    WindowBuffer,
    window_to_item,
)

logger = logging.getLogger(__name__)

TRENDING_TOPIC = "trending-results"
DLQ_TOPIC = "trending-writer-dlq"
GROUP_ID = "results-writer"
INSTANCE_ID = "results-writer-1"

# The writer retries with its own backoff and counts the tries, so boto3 must
# not retry underneath it.
NO_SDK_RETRIES = Config(retries={"total_max_attempts": 1})

# DynamoDB cannot be reached at all: refused or timed-out connections.
UNREACHABLE = (BotoConnectionError, HTTPClientError)

# While DynamoDB is unreachable the writer waits for as long as it takes: 1 s, 2 s, 4 s ... up to 30 s between tries, with jitter. max_attempts is not used here.
OUTAGE_BACKOFF = RetryPolicy(base_delay_s=1.0, max_delay_s=30.0)

# The writer does not poll Kafka while it waits out an outage. Heartbeats keep it in the group, but without a poll for max.poll.interval.ms (5 minutes by default) librdkafka leaves the group, and the next commit would fail.
MAX_POLL_INTERVAL_MS = 30 * 60 * 1000


def writer_consumer_config(
    bootstrap_servers: str, group_id: str = GROUP_ID, instance_id: str = INSTANCE_ID
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

    def __init__(self, producer: Any, topic: str = DLQ_TOPIC) -> None:
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
            logger.info(
                "Wrote window %s (%d programmes)", window.window_end, len(item["items"])
            )
        else:
            self.dead_letters.send(
                {
                    "window_end": window.window_end,
                    "item": item,
                    "error": failure.error,
                    "attempts": failure.attempts,
                    "dead_lettered_at": datetime.now(UTC).strftime(TIME_FORMAT),
                }
            )
            logger.error(
                "Dead-lettered window %s after %d tries: %s",
                window.window_end,
                failure.attempts,
                failure.error,
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
                    lambda: put_window(self.table, item), self.retry, sleep=self.sleep
                )
            except UNREACHABLE as exc:
                outage_tries += 1
                delay = self.outage_backoff.delay(outage_tries)
                logger.warning(
                    "DynamoDB unreachable (%s); not committing, try %d in %.1f s",
                    exc,
                    outage_tries + 1,
                    delay,
                )
                self.sleep(delay)

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
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--endpoint-url", default="http://localhost:8001")
    parser.add_argument("--table", default="trending")
    parser.add_argument("--topic", default=TRENDING_TOPIC)
    parser.add_argument("--dlq-topic", default=DLQ_TOPIC)
    parser.add_argument("--partition-key", default=PARTITION_KEY)
    parser.add_argument("--group-id", default=GROUP_ID)
    parser.add_argument("--instance-id", default=INSTANCE_ID)
    parser.add_argument("--idle-flush-seconds", type=float, default=2.0)
    parser.add_argument("--max-attempts", type=int, default=5)
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    consumer = Consumer(
        writer_consumer_config(args.bootstrap_servers, args.group_id, args.instance_id)
    )
    producer = Producer(
        {
            "bootstrap.servers": args.bootstrap_servers,
            "enable.idempotence": True,
            "acks": "all",
        }
    )
    dynamodb = dynamodb_resource(args.endpoint_url, config=NO_SDK_RETRIES)
    table = dynamodb.Table(args.table)

    writer = ResultsWriter(
        consumer,
        table,
        KafkaDeadLetters(producer, args.dlq_topic),
        RetryPolicy(max_attempts=args.max_attempts),
        idle_flush_s=args.idle_flush_seconds,
        partition_key=args.partition_key,
    )

    # Rows buffered for a partition this writer loses were never committed, so
    # whichever consumer gets the partition next reads them again.
    consumer.subscribe(
        [args.topic], on_revoke=lambda _consumer, _partitions: writer.buffer.clear()
    )

    stop_requested = False

    def request_stop(signum: int, frame: Any) -> None:
        nonlocal stop_requested
        stop_requested = True

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    logger.info("Writing %s to table %s", args.topic, args.table)
    try:
        writer.run(lambda: stop_requested)
    finally:
        # No commit here: a window that was only partly read is read again
        # from its first row on the next start.
        consumer.close()
        logger.info("Stopped")


if __name__ == "__main__":
    main()
