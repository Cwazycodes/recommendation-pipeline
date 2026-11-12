"""Apply the continue-watching job's upserts and deletes to the serving table.

Effectively once, like the results writer: a batch's offsets are committed
only after its changes are written (or dead-lettered), so a crash replays at
most the batch it was in, and replaying changes in order leaves the table as
it was. Within a batch only the last change to each item is written, which
is what applying them all in order would leave.
"""

from __future__ import annotations

import argparse
import json
import logging
import signal
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition

from realtime_recommendation_pipeline.common.continue_watching_table import (
    UPSERT,
    from_millis,
    item_key,
    progress_item,
)
from realtime_recommendation_pipeline.common.logs import configure_logging
from realtime_recommendation_pipeline.common.trending_table import (
    TIME_FORMAT,
    dynamodb_resource,
    millis_text,
)
from realtime_recommendation_pipeline.settings import (
    ContinueWatchingSettings,
    DynamoDBSettings,
    KafkaSettings,
    LogSettings,
    load,
)
from realtime_recommendation_pipeline.writer.results_writer import (
    NO_SDK_RETRIES,
    OUTAGE_BACKOFF,
    UNREACHABLE,
    KafkaDeadLetters,
    utc_now,
    writer_consumer_config,
)
from realtime_recommendation_pipeline.writer.retry import (
    RetryPolicy,
    WriteFailure,
    write_with_retry,
)

logger = logging.getLogger(__name__)

# At most this many changes per batch. BatchWriteItem takes 25 items a request,
# and boto3's batch writer splits a batch into requests.
BATCH_SIZE = 500

ItemKey = tuple[str, str]


def last_change_per_item(
    changes: Sequence[dict[str, Any]],
) -> dict[ItemKey, dict[str, Any]]:
    """The last change to each item, in the order the items first appear."""
    latest: dict[ItemKey, dict[str, Any]] = {}
    for change in changes:
        key = item_key(change)
        latest[(key["pk"], key["sk"])] = change
    return latest


@dataclass
class WriteSamples:
    """What make cw-bench measures, as (when written, value) pairs: for each
    change, seconds from its event being produced to its item being written,
    and for each batch, how long the write took."""

    latency_s: list[tuple[float, float]] = field(default_factory=list)
    write_s: list[tuple[float, float]] = field(default_factory=list)


class ContinueWatchingWriter:
    """Reads a batch of changes, writes it, then commits."""

    def __init__(
        self,
        consumer: Any,
        table: Any,
        dead_letters: Any,
        retry: RetryPolicy | None = None,
        sleep: Callable[[float], None] = time.sleep,
        outage_backoff: RetryPolicy = OUTAGE_BACKOFF,
        now: Callable[[], datetime] = utc_now,
        batch_size: int = BATCH_SIZE,
        poll_timeout_s: float = 1.0,
        samples: WriteSamples | None = None,
    ) -> None:
        self.consumer = consumer
        self.table = table
        self.dead_letters = dead_letters
        self.retry = retry or RetryPolicy()
        self.sleep = sleep
        self.outage_backoff = outage_backoff
        self.now = now
        self.batch_size = batch_size
        self.poll_timeout_s = poll_timeout_s
        self.samples = samples

    def poll_once(self) -> int:
        """Handle at most one batch; return how many changes it held."""
        messages = self.consumer.consume(self.batch_size, self.poll_timeout_s)

        changes = []
        next_offsets: dict[tuple[str, int], int] = {}
        for message in messages:
            if message.error():
                raise KafkaException(message.error())
            next_offsets[(message.topic(), message.partition())] = message.offset() + 1
            value = message.value()
            if value is not None:
                changes.append(json.loads(value))

        if changes:
            self.apply(changes)
        if next_offsets:
            self.commit(next_offsets)
        return len(changes)

    def apply(self, changes: list[dict[str, Any]]) -> None:
        """Write the batch, or dead-letter it, before its offsets are committed."""
        latest = list(last_change_per_item(changes).values())
        failure = self.write_when_reachable(latest)

        if failure is None:
            upserts = sum(change["op"] == UPSERT for change in latest)
            logger.info(
                "wrote changes",
                extra={
                    "changes": len(changes),
                    "items": len(latest),
                    "upserts": upserts,
                    "deletes": len(latest) - upserts,
                },
            )
            return

        self.dead_letters.send(
            {
                "changes": changes,
                "error": failure.error,
                "attempts": failure.attempts,
                "dead_lettered_at": self.now().strftime(TIME_FORMAT),
            }
        )
        logger.error(
            "dead-lettered changes",
            extra={
                "changes": len(changes),
                "attempts": failure.attempts,
                "error": failure.error,
            },
        )

    def write_when_reachable(
        self, changes: list[dict[str, Any]]
    ) -> WriteFailure | None:
        """Write with retries, first waiting out any time DynamoDB is unreachable.

        Writing a batch again is safe, so a try that failed part-way is simply
        repeated. Nothing is committed while it waits.
        """
        outage_tries = 0
        while True:
            try:
                return write_with_retry(
                    lambda: self.write(changes), self.retry, sleep=self.sleep
                )
            except UNREACHABLE as exc:
                outage_tries += 1
                delay = self.outage_backoff.delay(outage_tries)
                logger.warning(
                    "dynamodb unreachable, not committing",
                    extra={
                        "changes": len(changes),
                        "next_try": outage_tries + 1,
                        "retry_in_s": round(delay, 1),
                        "error": str(exc),
                    },
                )
                self.sleep(delay)

    def write(self, changes: list[dict[str, Any]]) -> None:
        """One try: put each upserted item and delete each deleted one."""
        written_at = millis_text(self.now())
        started = time.perf_counter()

        with self.table.batch_writer() as batch:
            for change in changes:
                if change["op"] == UPSERT:
                    item = {**progress_item(change), "written_at": written_at}
                    batch.put_item(Item=item)
                else:
                    batch.delete_item(Key=item_key(change))

        if self.samples is not None:
            # The changes are visible once the write has finished.
            done = self.now()
            at = done.timestamp()
            self.samples.write_s.append((at, time.perf_counter() - started))
            for change in changes:
                latency = done - from_millis(change["produced_at"])
                self.samples.latency_s.append((at, latency.total_seconds()))

    def commit(self, next_offsets: dict[tuple[str, int], int]) -> None:
        self.consumer.commit(
            offsets=[
                TopicPartition(topic, partition, offset)
                for (topic, partition), offset in next_offsets.items()
            ],
            asynchronous=False,
        )

    def run(self, should_stop: Callable[[], bool]) -> None:
        """Poll until should_stop() is true."""
        while not should_stop():
            self.poll_once()


def main(argv: Sequence[str] | None = None) -> None:
    """Hosts, topics, group and table come from the environment (settings.py)."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE)
    parser.add_argument("--max-attempts", type=int, default=5)
    args = parser.parse_args(argv)

    kafka = load(KafkaSettings)
    settings = load(ContinueWatchingSettings)
    dynamo = load(DynamoDBSettings)
    configure_logging("continue-watching-writer", load(LogSettings).log_level)

    consumer = Consumer(
        writer_consumer_config(
            kafka.bootstrap_servers,
            settings.writer_group_id,
            settings.writer_instance_id,
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

    writer = ContinueWatchingWriter(
        consumer,
        dynamodb.Table(dynamo.table),
        # A batch holds many users, so its DLQ record has no key.
        KafkaDeadLetters(producer, settings.writer_dlq_topic, key_field=None),
        RetryPolicy(max_attempts=args.max_attempts),
        batch_size=args.batch_size,
    )
    consumer.subscribe([settings.topic])

    stop_requested = False

    def request_stop(signum: int, frame: Any) -> None:
        nonlocal stop_requested
        stop_requested = True

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    logger.info(
        "writer started",
        extra={
            "topic": settings.topic,
            "table": dynamo.table,
            "group_id": settings.writer_group_id,
        },
    )
    try:
        writer.run(lambda: stop_requested)
    finally:
        consumer.close()
        logger.info("writer stopped")


if __name__ == "__main__":
    main()
