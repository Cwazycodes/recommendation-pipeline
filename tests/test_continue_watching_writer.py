import json
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Self

import boto3
import pytest
from botocore.exceptions import ClientError, EndpointConnectionError
from moto import mock_aws

from realtime_recommendation_pipeline.common.continue_watching_table import (
    in_progress,
    progress_item,
)
from realtime_recommendation_pipeline.common.trending_table import (
    create_table_if_missing,
    load_table_definition,
)
from realtime_recommendation_pipeline.writer.continue_watching_writer import (
    ContinueWatchingWriter,
    WriteSamples,
    last_change_per_item,
)
from realtime_recommendation_pipeline.writer.retry import RetryPolicy

ROOT = Path(__file__).resolve().parents[1]
TOPIC = "continue-watching-changes"
EMITTED_AT_MS = int(datetime(2026, 11, 2, 12, 0, tzinfo=UTC).timestamp() * 1000)
WRITTEN_AT = datetime(2026, 11, 2, 12, 0, 5, 250_000, tzinfo=UTC)


def change(
    op: str,
    user_id: str,
    programme_id: str,
    position_s: float,
    duration_s: float | None = 1800.0,
    seconds: int = 0,
) -> dict[str, Any]:
    event_time = 1_790_000_000_000 + seconds * 1000
    return {
        "op": op,
        "user_id": user_id,
        "programme_id": programme_id,
        "position_s": position_s,
        "duration_s": duration_s,
        "event_time": event_time,
        "event_id": f"{user_id}-{programme_id}-{seconds}",
        "produced_at": EMITTED_AT_MS - 3_000,
        "emitted_at": EMITTED_AT_MS,
    }


# Changes as the job publishes them, in order. u1 finishes prog-b.
CHANGES = [
    change("upsert", "u1", "prog-a", 0.0, seconds=0),
    change("upsert", "u1", "prog-b", 1500.0, seconds=0),
    change("upsert", "u2", "prog-a", 30.0, seconds=30),
    change("upsert", "u1", "prog-a", 30.0, seconds=30),
    change("delete", "u1", "prog-b", 1700.0, seconds=30),
    change("upsert", "u2", "prog-c", 90.5, duration_s=None, seconds=60),
    change("upsert", "u1", "prog-a", 60.0, seconds=60),
]


def throttled() -> ClientError:
    return ClientError(
        {
            "Error": {"Code": "ThrottlingException", "Message": "slow down"},
            "ResponseMetadata": {"HTTPStatusCode": 400},
        },
        "BatchWriteItem",
    )


class FakeMessage:
    def __init__(self, offset: int, value: dict[str, Any]) -> None:
        self._offset = offset
        self._value = value

    def error(self) -> None:
        return None

    def value(self) -> bytes:
        return json.dumps(self._value).encode()

    def topic(self) -> str:
        return TOPIC

    def partition(self) -> int:
        return 0

    def offset(self) -> int:
        return self._offset


class FakeConsumer:
    """One partition, read from the committed offset like a restarted consumer."""

    def __init__(self, committed: int = 0, fail_commits: int = 0) -> None:
        self.messages = [FakeMessage(offset, c) for offset, c in enumerate(CHANGES)]
        self.committed = committed
        self.position = committed
        self.fail_commits = fail_commits
        self.table: FakeTable | None = None
        self.items_at_commit: list[dict[tuple[str, str], dict[str, Any]]] = []

    def consume(self, num_messages: int, timeout: float) -> list[FakeMessage]:
        batch = self.messages[self.position : self.position + num_messages]
        self.position += len(batch)
        return batch

    def commit(self, offsets: list[Any], asynchronous: bool) -> None:
        assert asynchronous is False
        if self.table is not None:
            self.items_at_commit.append(dict(self.table.items))
        if self.fail_commits:
            self.fail_commits -= 1
            raise RuntimeError("writer killed before the commit")

        (offset,) = offsets
        assert (offset.topic, offset.partition) == (TOPIC, 0)
        self.committed = offset.offset


class FakeBatch:
    """boto3's batch writer: requests are sent when the with block ends."""

    def __init__(self, table: "FakeTable") -> None:
        self.table = table
        self.requests: list[tuple[str, dict[str, Any]]] = []

    def __enter__(self) -> Self:
        return self

    def put_item(self, Item: dict[str, Any]) -> None:
        self.requests.append(("put", Item))

    def delete_item(self, Key: dict[str, Any]) -> None:
        self.requests.append(("delete", Key))

    def __exit__(self, *exc_info: object) -> None:
        if exc_info[0] is not None:
            return
        self.table.writes += 1
        if self.table.errors:
            raise self.table.errors.pop(0)
        for kind, body in self.requests:
            key = (body["pk"], body["sk"])
            if kind == "put":
                self.table.items[key] = body
            else:
                self.table.items.pop(key, None)


class FakeTable:
    def __init__(self, errors: list[Exception] | None = None) -> None:
        self.items: dict[tuple[str, str], dict[str, Any]] = {}
        self.errors = errors or []
        self.writes = 0

    def batch_writer(self) -> FakeBatch:
        return FakeBatch(self)


class FakeDeadLetters:
    def __init__(self, error: Exception | None = None) -> None:
        self.records: list[dict[str, Any]] = []
        self.error = error

    def send(self, record: dict[str, Any]) -> None:
        if self.error is not None:
            raise self.error
        self.records.append(record)


def make_writer(
    consumer: FakeConsumer,
    table: FakeTable,
    dead_letters: FakeDeadLetters | None = None,
    batch_size: int = 3,
    samples: WriteSamples | None = None,
) -> ContinueWatchingWriter:
    consumer.table = table
    return ContinueWatchingWriter(
        consumer,
        table,
        dead_letters or FakeDeadLetters(),
        RetryPolicy(max_attempts=3),
        sleep=lambda _seconds: None,
        now=lambda: WRITTEN_AT,
        batch_size=batch_size,
        samples=samples,
    )


def drain(writer: ContinueWatchingWriter) -> None:
    while writer.poll_once():
        pass


def expected_items() -> dict[tuple[str, str], dict[str, Any]]:
    """The table after every change is applied in order, one at a time."""
    items: dict[tuple[str, str], dict[str, Any]] = {}
    for c in CHANGES:
        key = (f"USER#{c['user_id']}", c["programme_id"])
        if c["op"] == "upsert":
            items[key] = {**progress_item(c), "written_at": "2026-11-02T12:00:05.250Z"}
        else:
            items.pop(key, None)
    return items


# --- Items -------------------------------------------------------------------------


def test_upsert_item_has_the_position_and_times() -> None:
    item = progress_item(change("upsert", "u1", "prog-a", 1234.5, seconds=30))

    assert item == {
        "pk": "USER#u1",
        "sk": "prog-a",
        "position_s": Decimal("1234.5"),
        "duration_s": Decimal("1800.0"),
        "progress": Decimal("0.6858"),
        "last_watched_at": "2026-09-21T14:13:50.000Z",
        "event_id": "u1-prog-a-30",
        "generated_at": "2026-11-02T12:00:00.000Z",
        # 30 days from when Flink published the change.
        "ttl": EMITTED_AT_MS // 1000 + 30 * 24 * 60 * 60,
    }


def test_item_without_a_duration_has_no_progress() -> None:
    item = progress_item(change("upsert", "u1", "live", 90.5, duration_s=None))

    assert item["duration_s"] is None
    assert item["progress"] is None


def test_last_change_to_each_item_wins_within_a_batch() -> None:
    latest = last_change_per_item(CHANGES)

    assert latest[("USER#u1", "prog-a")]["position_s"] == 60.0
    assert latest[("USER#u1", "prog-b")]["op"] == "delete"
    assert len(latest) == 4


# --- Writing and committing ---------------------------------------------------------


def test_every_change_is_applied_in_order() -> None:
    table = FakeTable()

    drain(make_writer(FakeConsumer(), table))

    assert table.items == expected_items()
    assert ("USER#u1", "prog-b") not in table.items


def test_a_batch_is_written_before_its_offsets_are_committed() -> None:
    consumer = FakeConsumer()
    table = FakeTable()

    drain(make_writer(consumer, table, batch_size=3))

    # Batches of 3: offsets 3, 6 and 7 committed, each after its write.
    assert consumer.committed == len(CHANGES)
    assert ("USER#u2", "prog-a") in consumer.items_at_commit[0]
    assert ("USER#u1", "prog-b") not in consumer.items_at_commit[1]


@pytest.mark.parametrize("batch_size", [1, 2, 3, 7])
def test_killing_between_write_and_commit_leaves_the_same_table(
    batch_size: int,
) -> None:
    table = FakeTable()
    first_run = FakeConsumer(fail_commits=1)

    with pytest.raises(RuntimeError):
        drain(make_writer(first_run, table, batch_size=batch_size))
    assert first_run.committed == 0

    restarted = FakeConsumer(committed=first_run.committed)
    drain(make_writer(restarted, table, batch_size=batch_size))

    assert table.items == expected_items()
    assert restarted.committed == len(CHANGES)


def test_batch_that_keeps_failing_goes_to_the_dlq_and_is_committed() -> None:
    consumer = FakeConsumer()
    table = FakeTable(errors=[throttled()] * 3)
    dead_letters = FakeDeadLetters()

    drain(make_writer(consumer, table, dead_letters, batch_size=3))

    (record,) = dead_letters.records
    assert record["changes"] == CHANGES[:3]
    assert record["attempts"] == 3
    assert "ThrottlingException" in record["error"]
    assert table.writes == 3 + 2
    assert consumer.committed == len(CHANGES)


def test_writer_stops_without_committing_when_the_dlq_is_down() -> None:
    consumer = FakeConsumer()
    writer = make_writer(
        consumer,
        FakeTable([throttled()] * 3),
        FakeDeadLetters(RuntimeError("DLQ unavailable")),
    )

    with pytest.raises(RuntimeError):
        writer.poll_once()

    assert consumer.committed == 0


def test_unreachable_dynamodb_is_waited_out_without_committing() -> None:
    consumer = FakeConsumer()
    unreachable = EndpointConnectionError(endpoint_url="http://localhost:8001")
    table = FakeTable([unreachable] * 4)
    waits: list[tuple[float, int]] = []
    consumer.table = table
    writer = ContinueWatchingWriter(
        consumer,
        table,
        FakeDeadLetters(),
        RetryPolicy(max_attempts=3),
        sleep=lambda seconds: waits.append((seconds, consumer.committed)),
        now=lambda: WRITTEN_AT,
        batch_size=3,
    )

    writer.poll_once()

    assert len(waits) == 4
    assert all(committed == 0 for _, committed in waits)
    assert consumer.committed == 3
    assert ("USER#u2", "prog-a") in table.items


def test_samples_measure_from_the_event_to_the_write() -> None:
    samples = WriteSamples()

    drain(make_writer(FakeConsumer(), FakeTable(), batch_size=7, samples=samples))

    # One batch, and one sample for each of the 4 items it wrote.
    assert len(samples.write_s) == 1
    assert len(samples.latency_s) == 4
    # produced_at is 8.25 s before WRITTEN_AT.
    assert {latency for _, latency in samples.latency_s} == {8.25}


# --- The real table ----------------------------------------------------------------


@pytest.fixture
def table(monkeypatch: pytest.MonkeyPatch) -> Iterator[Any]:
    """moto with fake credentials, so no test can reach a real AWS account."""
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "eu-west-2")
    with mock_aws():
        dynamodb = boto3.resource("dynamodb")
        create_table_if_missing(
            dynamodb.meta.client,
            load_table_definition(ROOT / "infra" / "dynamodb" / "trending-table.json"),
        )
        yield dynamodb.Table("trending")


def test_changes_reach_a_real_table_and_read_back_newest_first(table: Any) -> None:
    consumer = FakeConsumer()
    writer = ContinueWatchingWriter(
        consumer,
        table,
        FakeDeadLetters(),
        sleep=lambda _seconds: None,
        now=lambda: WRITTEN_AT,
        batch_size=3,
    )

    drain(writer)

    now = from_ms(EMITTED_AT_MS)
    assert [item["sk"] for item in in_progress(table, "u1", now)] == ["prog-a"]
    (u1_prog_a,) = in_progress(table, "u1", now)
    assert u1_prog_a["position_s"] == 60
    assert u1_prog_a["progress"] == 0.0333
    assert [item["sk"] for item in in_progress(table, "u2", now)] == [
        "prog-c",
        "prog-a",
    ]
    assert in_progress(table, "nobody", now) == []


def test_expired_items_are_not_read(table: Any) -> None:
    table.put_item(Item=progress_item(change("upsert", "u1", "prog-a", 30.0)))

    later = from_ms(EMITTED_AT_MS) + timedelta(days=31)

    assert in_progress(table, "u1", later) == []


def test_every_page_of_a_long_list_is_read() -> None:
    class PagedTable:
        def __init__(self) -> None:
            self.calls: list[dict[str, Any]] = []

        def query(self, **arguments: Any) -> dict[str, Any]:
            self.calls.append(arguments)
            if "ExclusiveStartKey" not in arguments:
                return {
                    "Items": [{"sk": "a", "last_watched_at": "1"}],
                    "LastEvaluatedKey": {"pk": "USER#u1", "sk": "a"},
                }
            return {"Items": [{"sk": "b", "last_watched_at": "2"}]}

    paged = PagedTable()

    items = in_progress(paged, "u1", from_ms(EMITTED_AT_MS))

    assert [item["sk"] for item in items] == ["b", "a"]
    assert paged.calls[1]["ExclusiveStartKey"] == {"pk": "USER#u1", "sk": "a"}


def from_ms(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, UTC)
