import json
from datetime import UTC, datetime
from typing import Any

import pytest
from botocore.exceptions import (
    ClientError,
    EndpointConnectionError,
    ReadTimeoutError,
)

from realtime_recommendation_pipeline.writer.results_writer import (
    ResultsWriter,
    writer_consumer_config,
)
from realtime_recommendation_pipeline.writer.retry import (
    RetryPolicy,
    is_retryable,
    write_with_retry,
)
from realtime_recommendation_pipeline.writer.windows import (
    Window,
    WindowBuffer,
    window_to_item,
)

TOPIC = "trending-results"
EMITTED_AT_MS = int(datetime(2026, 11, 2, 12, 0, tzinfo=UTC).timestamp() * 1000)


def ranked_rows(window_end: str, scores: list[int]) -> list[dict[str, Any]]:
    """Rows as the Flink job writes them, ranks 1..n."""
    return [
        {
            "window_start": "2026-09-21T13:00:00Z",
            "window_end": window_end,
            "rank": rank,
            "programme_id": f"programme-{rank:04d}",
            "score": score,
            "max_produced_at": 1790000000000,
            "emitted_at": EMITTED_AT_MS + rank,
        }
        for rank, score in enumerate(scores, start=1)
    ]


# Three windows, each in its own transaction. Kafka puts a commit marker after
# each transaction, so offsets 3 and 6 hold no row.
WINDOWS = {
    "2026-09-21T14:00:00Z": ranked_rows("2026-09-21T14:00:00Z", [9, 5, 2]),
    "2026-09-21T14:05:00Z": ranked_rows("2026-09-21T14:05:00Z", [7, 3]),
    "2026-09-21T14:10:00Z": ranked_rows("2026-09-21T14:10:00Z", [8, 6, 1]),
}
OFFSETS = [0, 1, 2, 4, 5, 7, 8, 9]
ROWS = [row for rows in WINDOWS.values() for row in rows]


def throttled() -> ClientError:
    return ClientError(
        {
            "Error": {"Code": "ThrottlingException", "Message": "slow down"},
            "ResponseMetadata": {"HTTPStatusCode": 400},
        },
        "PutItem",
    )


class FakeMessage:
    def __init__(self, offset: int, row: dict[str, Any]) -> None:
        self._offset = offset
        self._row = row

    def error(self) -> None:
        return None

    def value(self) -> bytes:
        return json.dumps(self._row).encode()

    def topic(self) -> str:
        return TOPIC

    def partition(self) -> int:
        return 0

    def offset(self) -> int:
        return self._offset


class FakeConsumer:
    """One partition, read from the committed offset like a restarted consumer."""

    def __init__(self, committed: int = 0, fail_commits: int = 0) -> None:
        self.messages = [FakeMessage(o, row) for o, row in zip(OFFSETS, ROWS)]
        self.committed = committed
        self.position = committed
        self.fail_commits = fail_commits

    def poll(self, timeout_s: float) -> FakeMessage | None:
        for message in self.messages:
            if message.offset() >= self.position:
                self.position = message.offset() + 1
                return message
        return None

    def commit(self, offsets: list[Any], asynchronous: bool) -> None:
        assert asynchronous is False
        if self.fail_commits:
            self.fail_commits -= 1
            raise RuntimeError("writer killed before the commit")

        (offset,) = offsets
        assert (offset.topic, offset.partition) == (TOPIC, 0)
        self.committed = offset.offset


class FakeTable:
    def __init__(self, errors: list[Exception] | None = None) -> None:
        self.items: dict[str, dict[str, Any]] = {}
        self.errors = errors or []
        self.puts = 0

    def put_item(self, Item: dict[str, Any]) -> None:
        self.puts += 1
        if self.errors:
            raise self.errors.pop(0)
        self.items[Item["sk"]] = Item


class FakeDeadLetters:
    def __init__(self, error: Exception | None = None) -> None:
        self.records: list[dict[str, Any]] = []
        self.error = error

    def send(self, record: dict[str, Any]) -> None:
        if self.error is not None:
            raise self.error
        self.records.append(record)


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


def make_writer(
    consumer: FakeConsumer,
    table: FakeTable,
    dead_letters: FakeDeadLetters | None = None,
    clock: FakeClock | None = None,
) -> ResultsWriter:
    return ResultsWriter(
        consumer,
        table,
        dead_letters or FakeDeadLetters(),
        RetryPolicy(max_attempts=3),
        idle_flush_s=2.0,
        clock=clock or FakeClock(),
        sleep=lambda _seconds: None,
    )


def drain(writer: ResultsWriter, clock: FakeClock) -> None:
    """Read everything, then go idle long enough to flush the last window."""
    for _ in range(len(ROWS)):
        writer.poll_once()
    clock.now += 5
    writer.poll_once()


def expected_items() -> dict[str, dict[str, Any]]:
    return {end: window_to_item(Window(end, rows)) for end, rows in WINDOWS.items()}


# --- Buffering and items -----------------------------------------------------


def test_window_is_complete_when_a_row_from_the_next_window_arrives() -> None:
    buffer = WindowBuffer()
    first, second = list(WINDOWS)[:2]

    for offset, row in zip(OFFSETS[:3], WINDOWS[first]):
        assert buffer.add((TOPIC, 0), offset, row) is None

    complete = buffer.add((TOPIC, 0), 4, WINDOWS[second][0])

    assert complete is not None
    assert complete.window_end == first
    assert complete.rows == WINDOWS[first]


def test_committable_offsets_stop_at_the_window_being_read() -> None:
    buffer = WindowBuffer()
    first, second = list(WINDOWS)[:2]

    for offset, row in zip(OFFSETS[:3], WINDOWS[first]):
        buffer.add((TOPIC, 0), offset, row)
    assert buffer.committable_offsets() == {(TOPIC, 0): 0}

    buffer.add((TOPIC, 0), 4, WINDOWS[second][0])
    assert buffer.committable_offsets() == {(TOPIC, 0): 4}

    buffer.take()
    assert buffer.committable_offsets() == {(TOPIC, 0): 5}


def test_item_has_ranked_list_and_times_from_flink() -> None:
    window_end = "2026-09-21T14:00:00Z"
    item = window_to_item(Window(window_end, WINDOWS[window_end]))

    assert item["pk"] == "TRENDING#1h"
    assert item["sk"] == window_end
    assert item["items"] == [
        {"rank": 1, "programme_id": "programme-0001", "score": 9},
        {"rank": 2, "programme_id": "programme-0002", "score": 5},
        {"rank": 3, "programme_id": "programme-0003", "score": 2},
    ]
    # The latest emitted_at (rank 3's), not the time the writer ran.
    assert item["generated_at"] == "2026-11-02T12:00:00Z"
    assert item["ttl"] == (EMITTED_AT_MS + 3) // 1000 + 24 * 60 * 60


def test_replayed_rows_give_the_same_item() -> None:
    window_end = "2026-09-21T14:00:00Z"
    rows = WINDOWS[window_end]

    first = window_to_item(Window(window_end, rows))
    replayed = window_to_item(Window(window_end, list(reversed(rows)) + rows[:1]))

    assert replayed == first


# --- Retries -----------------------------------------------------------------


def test_backoff_doubles_up_to_the_cap() -> None:
    policy = RetryPolicy(base_delay_s=0.1, max_delay_s=1.0)

    def at_most(low: float, high: float) -> float:
        return high

    ceilings = [policy.delay(attempt, at_most) for attempt in range(1, 7)]

    assert ceilings == pytest.approx([0.1, 0.2, 0.4, 0.8, 1.0, 1.0])
    assert policy.delay(3, lambda low, high: low) == 0


@pytest.mark.parametrize(
    ("code", "status", "retryable"),
    [
        ("ThrottlingException", 400, True),
        ("ProvisionedThroughputExceededException", 400, True),
        ("RequestLimitExceeded", 400, True),
        ("InternalServerError", 500, True),
        ("ServiceUnavailable", 503, True),
        ("ValidationException", 400, False),
        ("ResourceNotFoundException", 400, False),
    ],
)
def test_only_throttling_and_server_errors_are_retried(
    code: str, status: int, retryable: bool
) -> None:
    error = ClientError(
        {"Error": {"Code": code}, "ResponseMetadata": {"HTTPStatusCode": status}},
        "PutItem",
    )

    assert is_retryable(error) is retryable


def test_write_is_retried_until_it_succeeds() -> None:
    table = FakeTable(errors=[throttled(), throttled()])
    sleeps: list[float] = []

    failure = write_with_retry(
        lambda: table.put_item(Item={"sk": "w"}), RetryPolicy(), sleep=sleeps.append
    )

    assert failure is None
    assert table.puts == 3
    assert len(sleeps) == 2


def test_write_gives_up_after_max_attempts() -> None:
    table = FakeTable(errors=[throttled()] * 10)
    sleeps: list[float] = []

    failure = write_with_retry(
        lambda: table.put_item(Item={"sk": "w"}),
        RetryPolicy(max_attempts=4),
        sleep=sleeps.append,
    )

    assert failure is not None
    assert failure.attempts == 4
    assert "ThrottlingException" in failure.error
    assert table.puts == 4
    assert len(sleeps) == 3


def test_error_that_cannot_succeed_is_not_retried() -> None:
    invalid = ClientError(
        {"Error": {"Code": "ValidationException"}, "ResponseMetadata": {}},
        "PutItem",
    )
    table = FakeTable(errors=[invalid])

    failure = write_with_retry(
        lambda: table.put_item(Item={"sk": "w"}), RetryPolicy(), sleep=lambda _s: None
    )

    assert failure is not None
    assert failure.attempts == 1


# --- The writer: commits, crashes and dead letters ---------------------------


def test_window_is_written_before_its_offsets_are_committed() -> None:
    consumer, table = FakeConsumer(), FakeTable()
    writer = make_writer(consumer, table)

    for _ in range(3):
        writer.poll_once()
    assert table.items == {}
    assert consumer.committed == 0

    writer.poll_once()  # first row of the second window

    assert list(table.items) == ["2026-09-21T14:00:00Z"]
    # Up to the second window's first row, which is not written yet.
    assert consumer.committed == 4


def test_last_window_is_flushed_after_the_idle_timeout() -> None:
    clock = FakeClock()
    consumer, table = FakeConsumer(), FakeTable()
    writer = make_writer(consumer, table, clock=clock)

    for _ in range(len(ROWS) + 1):
        writer.poll_once()
    assert "2026-09-21T14:10:00Z" not in table.items

    clock.now += 2
    writer.poll_once()

    assert table.items == expected_items()
    assert consumer.committed == 10


@pytest.mark.parametrize("killed_after", range(len(ROWS) + 1))
def test_killing_and_restarting_the_writer_leaves_correct_items(
    killed_after: int,
) -> None:
    table = FakeTable()

    first_run = FakeConsumer()
    writer = make_writer(first_run, table)
    for _ in range(killed_after):
        writer.poll_once()
    # Killed here: nothing more is written or committed.

    clock = FakeClock()
    restarted = FakeConsumer(committed=first_run.committed)
    drain(make_writer(restarted, table, clock=clock), clock)

    assert table.items == expected_items()
    for item in table.items.values():
        ranks = [entry["rank"] for entry in item["items"]]
        assert ranks == sorted(set(ranks))
    # Nothing was written twice: the partly read window had not been written.
    assert table.puts == len(WINDOWS)


def test_kill_between_write_and_commit_rewrites_the_same_item() -> None:
    table = FakeTable()
    writer = make_writer(FakeConsumer(fail_commits=1), table)

    with pytest.raises(RuntimeError):
        for _ in range(4):
            writer.poll_once()

    first = "2026-09-21T14:00:00Z"
    written_before_kill = table.items[first]

    clock = FakeClock()
    drain(make_writer(FakeConsumer(committed=0), table, clock=clock), clock)

    assert table.items == expected_items()
    assert table.items[first] == written_before_kill
    assert table.puts == len(WINDOWS) + 1


def test_window_that_keeps_failing_goes_to_the_dlq_and_is_committed() -> None:
    clock = FakeClock()
    consumer = FakeConsumer()
    table = FakeTable(errors=[throttled()] * 3)
    dead_letters = FakeDeadLetters()

    drain(make_writer(consumer, table, dead_letters, clock), clock)

    (record,) = dead_letters.records
    assert record["window_end"] == "2026-09-21T14:00:00Z"
    assert record["attempts"] == 3
    assert record["item"] == expected_items()["2026-09-21T14:00:00Z"]
    assert "ThrottlingException" in record["error"]
    assert list(table.items) == ["2026-09-21T14:05:00Z", "2026-09-21T14:10:00Z"]
    assert consumer.committed == 10


def test_writer_stops_without_committing_when_the_dlq_is_down() -> None:
    consumer = FakeConsumer()
    writer = make_writer(
        consumer,
        FakeTable([throttled()] * 3),
        FakeDeadLetters(RuntimeError("DLQ unavailable")),
    )

    with pytest.raises(RuntimeError):
        for _ in range(4):
            writer.poll_once()

    assert consumer.committed == 0


@pytest.mark.parametrize(
    "unreachable",
    [
        EndpointConnectionError(endpoint_url="http://localhost:8001"),
        ReadTimeoutError(endpoint_url="http://localhost:8001"),
    ],
    ids=["connection-refused", "read-timeout"],
)
def test_unreachable_dynamodb_is_waited_out_without_committing(
    unreachable: Exception,
) -> None:
    consumer = FakeConsumer()
    table = FakeTable([unreachable] * 4)
    dead_letters = FakeDeadLetters()
    waits: list[tuple[float, int]] = []
    writer = ResultsWriter(
        consumer,
        table,
        dead_letters,
        RetryPolicy(max_attempts=3),
        sleep=lambda seconds: waits.append((seconds, consumer.committed)),
        outage_backoff=RetryPolicy(base_delay_s=1.0, max_delay_s=30.0),
    )

    for _ in range(4):  # the first row of the next window completes the first
        writer.poll_once()

    # Four failed tries, each followed by a wait, and nothing committed while
    # waiting; the fifth try writes the window and only then commits.
    assert len(waits) == 4
    assert all(committed == 0 for _seconds, committed in waits)
    assert all(0 <= seconds <= 30 for seconds, _committed in waits)
    assert dead_letters.records == []
    assert list(table.items) == ["2026-09-21T14:00:00Z"]
    assert consumer.committed == 4


def test_writer_can_wait_out_long_outages_without_leaving_its_group() -> None:
    config = writer_consumer_config("localhost:9092", "results-writer", "writer-1")

    assert int(config["max.poll.interval.ms"]) == 30 * 60 * 1000


def test_writer_uses_the_partition_key_it_is_given() -> None:
    clock = FakeClock()
    table = FakeTable()
    writer = ResultsWriter(
        FakeConsumer(),
        table,
        FakeDeadLetters(),
        clock=clock,
        sleep=lambda _seconds: None,
        partition_key="TRENDING#2m",
    )

    drain(writer, clock)

    assert sorted(table.items) == sorted(WINDOWS)
    assert {item["pk"] for item in table.items.values()} == {"TRENDING#2m"}
