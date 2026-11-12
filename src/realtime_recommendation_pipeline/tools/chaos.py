"""Failure and recovery drills on the local stack (make chaos).

Each drill breaks one part of the running pipeline on purpose and checks the
outcome the design promises:

- taskmanager: SIGKILL the Flink TaskManager mid-stream. The job restores
  from its last checkpoint and its committed output still matches the oracle.
- kafka: restart the Kafka broker mid-stream. The producer and the job
  recover, no event is lost, and the output still matches the oracle.
- writer: SIGKILL the results writer mid-batch, three times. The table ends
  with one complete item per window and no duplicates.
- dynamodb: stop DynamoDB Local for 30 s. The writer backs off, commits
  nothing while it waits, then catches up.
- continue-watching: run the continue-watching job and its writer, and
  SIGKILL the TaskManager and then the writer mid-stream. The table ends
  with exactly the programmes the oracle has in progress.
- schema: the producers move from v1 of the event schema to v2 halfway
  through. The job, which reads v1, runs on without a restart, dead-letters
  nothing, and its output still matches the oracle.


The drills have their own topics and partition key, but they kill, restart
and stop the shared containers, so anything else on the stack is disrupted
too. The results are written to docs/reliability.md.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import subprocess
import sys
import threading
import time
import uuid
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from botocore.exceptions import BotoCoreError, ClientError
from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition

from realtime_recommendation_pipeline.common.continue_watching_table import (
    USER_KEY_PREFIX,
    from_millis,
)
from realtime_recommendation_pipeline.common.event_schemas import (
    LATEST_VERSION,
    schema_text,
    subject,
)
from realtime_recommendation_pipeline.common.kafka import downstream_consumer_config
from realtime_recommendation_pipeline.common.trending_table import (
    TIME_FORMAT,
    create_table_if_missing,
    dynamodb_resource,
    load_table_definition,
    millis_text,
    plain,
    recreate_table,
)
from realtime_recommendation_pipeline.continue_watching_oracle import (
    InProgress,
    continue_watching_oracle,
)
from realtime_recommendation_pipeline.flink_jobs.event_time import (
    MAX_OUT_OF_ORDERNESS_S,
)
from realtime_recommendation_pipeline.flink_jobs.trending import TrendingConfig
from realtime_recommendation_pipeline.producer.generate import generate_events
from realtime_recommendation_pipeline.producer.kafka_producer import produce_events
from realtime_recommendation_pipeline.settings import (
    ContinueWatchingSettings,
    environment,
    load,
)
from realtime_recommendation_pipeline.tools.check_trending import read_rows
from realtime_recommendation_pipeline.tools.check_writer import (
    compare,
    expected_items,
    table_items,
)
from realtime_recommendation_pipeline.tools.demo import (
    REPO_ROOT,
    TABLE_DEFINITION,
    DemoError,
    DemoSettings,
    flink_request,
    job_is_running,
    recreate_topics,
    stop_job,
    submit_job,
    wait_for_services,
    wait_until,
)
from realtime_recommendation_pipeline.trending_oracle import (
    TrendingRow,
    trending_oracle,
)

EVENTS_TOPIC = "viewing-events-chaos"
TRENDING_TOPIC = "trending-results-chaos"
EVENTS_DLQ_TOPIC = "viewing-events-dlq-chaos"
WRITER_DLQ_TOPIC = "trending-writer-dlq-chaos"
JOB_TOPICS = {EVENTS_TOPIC: 6, TRENDING_TOPIC: 1, EVENTS_DLQ_TOPIC: 6}
WRITER_TOPICS = {TRENDING_TOPIC: 1, WRITER_DLQ_TOPIC: 1}
PARTITION_KEY = "CHAOS#1h"
# The continue-watching drill: the second job and its writer, with topics and a
# table of their own.
CW_EVENTS_TOPIC = "viewing-events-cw-chaos"
CW_TOPIC = "continue-watching-chaos"
CW_DLQ_TOPIC = "continue-watching-writer-dlq-chaos"
CW_TOPICS = {CW_EVENTS_TOPIC: 6, CW_TOPIC: 1, CW_DLQ_TOPIC: 1}
CW_TABLE = "continue-watching-chaos"
CW_JOB_ARGS = ["--source-topic", CW_EVENTS_TOPIC, "--continue-watching-topic", CW_TOPIC]

# The production job (1-hour windows every 5 minutes) on the chaos topics.
JOB_ARGS = [
    "--source-topic", EVENTS_TOPIC,
    "--trending-topic", TRENDING_TOPIC,
    "--dlq-topic", EVENTS_DLQ_TOPIC,
]  # fmt: skip

# 6,000 generated events with 5% duplicates: about 100 minutes of event time,
# so 20-odd windows, produced in two minutes.
RATE = 50.0
DURATION_S = 120.0
SEED = 42
DUP_RATE = 0.05

RESULTS_DOC = REPO_ROOT / "docs" / "reliability.md"
START_MARKER = "<!-- chaos-results:start -->"
END_MARKER = "<!-- chaos-results:end -->"

Say = Callable[[str], None]


@dataclass
class DrillReport:
    """What a drill saw, and what it expected but did not see."""

    observed: list[str] = field(default_factory=list)
    problems: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class DrillResult:
    name: str
    title: str
    report: DrillReport
    duration_s: float

    @property
    def passed(self) -> bool:
        return not self.report.problems


# --- Pure helpers ------------------------------------------------------------


def epoch_ms(iso: str) -> int:
    parsed = datetime.strptime(iso, TIME_FORMAT).replace(tzinfo=UTC)
    return int(parsed.timestamp() * 1000)


def iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, UTC).strftime(TIME_FORMAT)


def to_trending_row(row: Mapping[str, Any]) -> TrendingRow:
    """A trending-results message as the oracle's row type."""
    return TrendingRow(
        window_start=epoch_ms(row["window_start"]),
        window_end=epoch_ms(row["window_end"]),
        rank=row["rank"],
        programme_id=row["programme_id"],
        score=row["score"],
        max_produced_at=row["max_produced_at"],
    )


def describe(row: TrendingRow) -> str:
    return f"{iso(row.window_end)} #{row.rank} {row.programme_id} ({row.score})"


def compare_with_oracle(
    rows: Sequence[Mapping[str, Any]], expected: Sequence[TrendingRow]
) -> list[str]:
    """Differences between the committed rows and the oracle.

    The oracle fires every window at the end of its input; the job only fires
    windows its watermark has passed. So only windows up to the last one the
    job committed are compared.
    """
    actual = [to_trending_row(row) for row in rows]
    if not actual:
        return ["no trending rows were committed"]

    last_end = max(row.window_end for row in actual)
    wanted = {row for row in expected if row.window_end <= last_end}
    problems = []

    repeated = len(actual) - len(set(actual))
    if repeated:
        problems.append(f"{repeated} rows were committed more than once")

    missing = sorted(wanted - set(actual))
    if missing:
        problems.append(
            f"{len(missing)} oracle rows are missing, e.g. {describe(missing[0])}"
        )

    extra = sorted(set(actual) - wanted)
    if extra:
        problems.append(
            f"{len(extra)} rows are not in the oracle, e.g. {describe(extra[0])}"
        )

    return problems


def synthetic_windows(
    count: int, emitted_at_ms: int, seed: int = 7
) -> list[list[dict[str, Any]]]:
    """Ranked rows shaped like the job's output: 1-hour windows every 5 minutes."""
    rng = random.Random(seed)
    first_end = datetime(2026, 9, 21, 14, 0, tzinfo=UTC)
    windows = []

    for index in range(count):
        end = first_end + timedelta(minutes=5 * index)
        programmes = rng.sample(range(1, 501), 20)
        scores = sorted(rng.sample(range(1, 300), 20), reverse=True)
        windows.append(
            [
                {
                    "window_start": (end - timedelta(hours=1)).strftime(TIME_FORMAT),
                    "window_end": end.strftime(TIME_FORMAT),
                    "rank": rank,
                    "programme_id": f"programme-{programme:04d}",
                    "score": score,
                    "max_produced_at": emitted_at_ms,
                    "emitted_at": emitted_at_ms,
                }
                for rank, (programme, score) in enumerate(zip(programmes, scores), 1)
            ]
        )

    return windows


# A listed programme, by (user_id, programme_id): position, duration, last watched.
Listing = dict[tuple[str, str], tuple[Any, ...]]


def listed_programmes(items: Iterable[Mapping[str, Any]]) -> Listing:
    """The continue-watching items in a table, as the oracle describes them."""
    return {
        (item["pk"].removeprefix(USER_KEY_PREFIX), item["sk"]): (
            item["position_s"],
            item["duration_s"],
            item["last_watched_at"],
        )
        for item in items
    }


def oracle_programmes(expected: Iterable[InProgress]) -> Listing:
    return {
        (row.user_id, row.programme_id): (
            row.position_s,
            row.duration_s,
            millis_text(from_millis(row.event_time)),
        )
        for row in expected
    }


def compare_listings(actual: Listing, expected: Listing) -> list[str]:
    """Differences between the table's programmes and the oracle's."""

    def name(key: tuple[str, str]) -> str:
        return f"{key[0]} {key[1]}"

    problems = []
    missing = sorted(expected.keys() - actual.keys())
    if missing:
        problems.append(
            f"{len(missing)} in-progress programmes are missing, e.g. {name(missing[0])}"
        )
    extra = sorted(actual.keys() - expected.keys())
    if extra:
        problems.append(
            f"{len(extra)} programmes are listed but should not be, e.g. {name(extra[0])}"
        )
    wrong = sorted(
        key for key in actual.keys() & expected.keys() if actual[key] != expected[key]
    )
    if wrong:
        key = wrong[0]
        problems.append(
            f"{len(wrong)} programmes have the wrong position, e.g. {name(key)}: "
            f"{actual[key]} instead of {expected[key]}"
        )
    return problems


def first_few(problems: Sequence[str], keep: int = 3) -> list[str]:
    """The first problems, and how many more there were."""
    if len(problems) <= keep:
        return list(problems)
    return [*problems[:keep], f"and {len(problems) - keep} more"]


def render_results(results: Sequence[DrillResult], recorded_on: str) -> str:
    """The recorded-results block for docs/reliability.md."""
    lines = [
        START_MARKER,
        "",
        f"Recorded with `make chaos` {recorded_on}.",
        "",
        "| Drill | Result | Observed |",
        "|---|---|---|",
    ]
    for result in results:
        observed = result.report.observed + [
            f"**{problem}**" for problem in result.report.problems
        ]
        cell = "<br>".join(item.replace("|", "/") for item in observed)
        verdict = "Pass" if result.passed else "**Fail**"
        lines.append(f"| {result.title} | {verdict} | {cell} |")

    lines += ["", END_MARKER]
    return "\n".join(lines)


def record_results(doc: Path, block: str) -> None:
    """Replace the text between the result markers, keeping the rest."""
    text = doc.read_text()
    start = text.index(START_MARKER)
    end = text.index(END_MARKER) + len(END_MARKER)
    doc.write_text(text[:start] + block + text[end:])


# --- Talking to the stack ----------------------------------------------------


def compose(*args: str) -> None:
    """Run a docker compose command for this repo's stack."""
    subprocess.run(
        ["docker", "compose", *args], cwd=REPO_ROOT, check=True, capture_output=True
    )


def end_offset(bootstrap_servers: str, topic: str) -> int:
    """Total end offset of a topic: rows plus transaction markers."""
    consumer = Consumer(
        {"bootstrap.servers": bootstrap_servers, "group.id": f"chaos-{uuid.uuid4()}"}
    )
    try:
        partitions = consumer.list_topics(topic, timeout=10).topics[topic].partitions
        return sum(
            consumer.get_watermark_offsets(TopicPartition(topic, partition), 10)[1]
            for partition in partitions
        )
    finally:
        consumer.close()


def committed_offset(bootstrap_servers: str, group_id: str, topic: str) -> int:
    """The offset a consumer group has committed on partition 0 of topic."""
    consumer = Consumer({"bootstrap.servers": bootstrap_servers, "group.id": group_id})
    try:
        (committed,) = consumer.committed([TopicPartition(topic, 0)], timeout=10)
        return max(committed.offset, 0)
    finally:
        consumer.close()


def wait_until_quiet(
    bootstrap_servers: str, topic: str, quiet_s: float = 30, timeout_s: float = 600
) -> None:
    """Wait until nothing new has been written to topic for quiet_s."""
    deadline = time.monotonic() + timeout_s
    last, since = end_offset(bootstrap_servers, topic), time.monotonic()
    while time.monotonic() - since < quiet_s:
        if time.monotonic() > deadline:
            raise DemoError(f"{topic} was still growing after {timeout_s:.0f} s")
        time.sleep(5)
        now = end_offset(bootstrap_servers, topic)
        if now != last:
            last, since = now, time.monotonic()


def checkpoint_counts(flink_url: str, job_id: str) -> dict[str, Any]:
    checkpoints = flink_request(f"{flink_url}/jobs/{job_id}/checkpoints")
    restored = (checkpoints.get("latest") or {}).get("restored") or {}
    return {**checkpoints["counts"], "restored_id": restored.get("id")}


def read_committed(bootstrap_servers: str, topic: str) -> list[dict[str, Any]]:
    config = downstream_consumer_config(
        bootstrap_servers, f"chaos-check-{uuid.uuid4()}"
    )
    return read_rows(config, topic, idle_s=10)


class EventProducer(threading.Thread):
    """The generated events for the job drills, at a steady rate."""

    def __init__(
        self,
        settings: DemoSettings,
        topic: str = EVENTS_TOPIC,
        schema_versions: Sequence[int] = (LATEST_VERSION,),
    ) -> None:
        super().__init__(name="chaos-producer", daemon=True)
        self.settings = settings
        self.topic = topic
        self.schema_versions = schema_versions
        self.stats: dict[str, int] | None = None

    def run(self) -> None:
        self.stats = produce_events(
            bootstrap_servers=self.settings.bootstrap_servers,
            rate=RATE,
            duration=DURATION_S,
            seed=SEED,
            dup_rate=DUP_RATE,
            topic=self.topic,
            registry=self.settings.schema_registry,
            schema_versions=self.schema_versions,
        )


class WindowPublisher(threading.Thread):
    """Ranked windows in Kafka transactions, the way the Flink job writes them."""

    def __init__(
        self,
        bootstrap_servers: str,
        windows: Sequence[Sequence[dict[str, Any]]],
        every_s: float,
    ) -> None:
        super().__init__(name="chaos-windows", daemon=True)
        self.bootstrap_servers = bootstrap_servers
        self.windows = windows
        self.every_s = every_s

    def run(self) -> None:
        producer = Producer(
            {
                "bootstrap.servers": self.bootstrap_servers,
                "transactional.id": f"chaos-windows-{uuid.uuid4()}",
            }
        )
        producer.init_transactions(30)
        for rows in self.windows:
            producer.begin_transaction()
            for row in rows:
                producer.produce(TRENDING_TOPIC, json.dumps(row).encode())
            producer.commit_transaction(30)
            time.sleep(self.every_s)


class WriterProcess:
    """The results writer as a child process, so a drill can really kill it."""

    # The log line for each write it counts.
    written_message = "wrote window"

    def __init__(self, settings: DemoSettings, group_id: str) -> None:
        self.command = [
            sys.executable,
            "-m",
            "realtime_recommendation_pipeline.writer.results_writer",
            "--idle-flush-seconds",
            "1",
        ]
        # The writer reads its settings from the environment, like make writer.
        kafka = settings.kafka.model_copy(
            update={
                "trending_topic": TRENDING_TOPIC,
                "writer_dlq_topic": WRITER_DLQ_TOPIC,
                "writer_group_id": group_id,
                "writer_instance_id": group_id,
            }
        )
        dynamo = settings.dynamo.model_copy(update={"partition_key": PARTITION_KEY})
        self.env = {
            **os.environ,
            **environment(kafka),
            **environment(dynamo),
            **environment(settings.metrics),
            "PYTHONUNBUFFERED": "1",
        }
        self.written = 0
        self.unreachable = 0
        self.process: subprocess.Popen[str] | None = None

    def start(self) -> None:
        self.process = subprocess.Popen(
            self.command,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
            env=self.env,
        )
        threading.Thread(
            target=self._read_log, args=(self.process,), daemon=True
        ).start()

    def _read_log(self, process: subprocess.Popen[str]) -> None:
        assert process.stderr is not None
        for line in process.stderr:
            try:
                message = json.loads(line).get("message", "")
            except ValueError:
                continue
            if message == self.written_message:
                self.written += 1
            elif message.startswith("dynamodb unreachable"):
                self.unreachable += 1

    def kill(self) -> None:
        """SIGKILL: no shutdown code runs, like a crash."""
        if self.process is not None:
            self.process.kill()
            self.process.wait()

    def stop(self) -> None:
        if self.process is not None and self.process.poll() is None:
            self.process.terminate()
            self.process.wait(timeout=30)


class ContinueWatchingWriterProcess(WriterProcess):
    """The continue-watching writer as a child process, on the drill's topic
    and table. Its writes are batches."""

    written_message = "wrote changes"

    def __init__(self, settings: DemoSettings, group_id: str) -> None:
        super().__init__(settings, group_id)
        self.command = [
            sys.executable,
            "-m",
            "realtime_recommendation_pipeline.writer.continue_watching_writer",
        ]
        continue_watching = load(ContinueWatchingSettings).model_copy(
            update={
                "topic": CW_TOPIC,
                "writer_dlq_topic": CW_DLQ_TOPIC,
                "writer_group_id": group_id,
                "writer_instance_id": group_id,
            }
        )
        dynamo = settings.dynamo.model_copy(update={"table": CW_TABLE})
        self.env = {**self.env, **environment(continue_watching), **environment(dynamo)}


# --- The drills ----------------------------------------------------------------


def run_job_drill(
    settings: DemoSettings,
    say: Say,
    disrupt: Callable[[str], DrillReport],
    schema_versions: Sequence[int] = (LATEST_VERSION,),
) -> DrillReport:
    """Run the job on generated events, break something, compare with the oracle."""
    report = DrillReport()
    recreate_topics(settings.bootstrap_servers, JOB_TOPICS)
    job_id = submit_job(JOB_ARGS)
    try:
        wait_until(
            "The chaos job did not start running",
            lambda: job_is_running(settings.flink_url, job_id),
            timeout_s=180,
        )
        producer = EventProducer(settings, schema_versions=schema_versions)
        producer.start()
        say(
            f"Job running; producing {RATE * DURATION_S:.0f} events over {DURATION_S:.0f} s"
        )

        # Break things only once a checkpoint exists and results are committed.
        wait_until(
            "No checkpoint completed with results committed",
            lambda: (
                checkpoint_counts(settings.flink_url, job_id)["completed"] >= 1
                and end_offset(settings.bootstrap_servers, TRENDING_TOPIC) > 0
            ),
            timeout_s=180,
        )
        disruption = disrupt(job_id)
        report.observed += disruption.observed
        report.problems += disruption.problems

        wait_until(
            "The job did not run again after the failure",
            lambda: job_is_running(settings.flink_url, job_id),
            timeout_s=300,
        )
        producer.join(timeout=DURATION_S + 300)
        if producer.stats is None:
            raise DemoError("The producer did not finish")

        say("Waiting for the job to commit its last windows")
        wait_until_quiet(settings.bootstrap_servers, TRENDING_TOPIC)

        events = generate_events(int(RATE * DURATION_S), seed=SEED, dup_rate=DUP_RATE)
        delivered = producer.stats["delivered"]
        in_topic = end_offset(settings.bootstrap_servers, EVENTS_TOPIC)
        report.observed.append(
            f"{delivered} of {len(events)} events delivered, {in_topic} in the topic"
        )
        if not delivered == in_topic == len(events) or producer.stats["failures"]:
            report.problems.append(
                f"events lost or repeated: produced {len(events)}, delivered "
                f"{delivered}, failures {producer.stats['failures']}, in topic {in_topic}"
            )

        trending = TrendingConfig()
        expected = trending_oracle(
            events,
            window_size_ms=trending.window_size_seconds * 1000,
            window_slide_ms=trending.window_slide_seconds * 1000,
            top_n=trending.top_n,
            max_out_of_orderness_ms=MAX_OUT_OF_ORDERNESS_S * 1000,
        )
        rows = read_committed(settings.bootstrap_servers, TRENDING_TOPIC)
        mismatches = compare_with_oracle(rows, expected)
        report.problems += mismatches
        if not mismatches:
            windows = len({row["window_end"] for row in rows})
            report.observed.append(
                f"{len(rows)} committed rows in {windows} windows match the oracle, "
                "none repeated"
            )
    finally:
        try:
            stop_job(settings.flink, job_id)
        except (OSError, DemoError) as exc:
            say(f"Could not stop the chaos job {job_id}: {exc}")

    return report


def kill_taskmanager(settings: DemoSettings, say: Say, job_id: str) -> DrillReport:
    """SIGKILL the TaskManager, start it 10 s later and wait for the job to restore."""
    report = DrillReport()
    before = checkpoint_counts(settings.flink_url, job_id)
    try:
        say("Killing the TaskManager with SIGKILL")
        killed_at = time.monotonic()
        compose("kill", "-s", "SIGKILL", "flink-taskmanager")
        # Docker does not restart a container stopped by `docker kill`, so
        # bring it back the way an orchestrator would.
        time.sleep(10)
    finally:
        compose("start", "flink-taskmanager")

    wait_until(
        "The job did not restore from a checkpoint",
        lambda: checkpoint_counts(settings.flink_url, job_id)["restored"] >= 1,
        timeout_s=300,
    )
    wait_until(
        "The job did not run again",
        lambda: job_is_running(settings.flink_url, job_id),
        timeout_s=300,
    )
    after = checkpoint_counts(settings.flink_url, job_id)
    report.observed.append(
        f"TaskManager killed with {before['completed']} checkpoint(s) completed; "
        f"running again {time.monotonic() - killed_at:.0f} s later, restored from "
        f"checkpoint {after['restored_id']}"
    )
    return report


def taskmanager_drill(settings: DemoSettings, say: Say) -> DrillReport:
    return run_job_drill(
        settings, say, lambda job_id: kill_taskmanager(settings, say, job_id)
    )


def kafka_drill(settings: DemoSettings, say: Say) -> DrillReport:
    def restart_kafka(job_id: str) -> DrillReport:
        report = DrillReport()
        say("Restarting the Kafka broker")
        started = time.monotonic()
        try:
            compose("restart", "kafka")
        finally:
            compose("start", "kafka")

        wait_until(
            "Kafka did not come back",
            lambda: end_offset(settings.bootstrap_servers, EVENTS_TOPIC) >= 0,
            timeout_s=300,
        )
        restored = checkpoint_counts(settings.flink_url, job_id)["restored"]
        report.observed.append(
            f"broker back after {time.monotonic() - started:.0f} s; the job "
            + (
                f"restarted {restored} time(s) from a checkpoint"
                if restored
                else "rode it out without restarting"
            )
        )
        return report

    return run_job_drill(settings, say, restart_kafka)


def clear_partition(settings: DemoSettings) -> Any:
    """The table, without items left under PARTITION_KEY by earlier runs."""
    table = dynamodb_resource(settings.dynamodb_endpoint).Table(settings.dynamo.table)
    with table.batch_writer() as batch:
        for window_end in table_items(table, PARTITION_KEY):
            batch.delete_item(Key={"pk": PARTITION_KEY, "sk": window_end})
    return table


def check_table(settings: DemoSettings, table: Any, report: DrillReport) -> None:
    rows = read_committed(settings.bootstrap_servers, TRENDING_TOPIC)
    expected = expected_items(rows, PARTITION_KEY)
    actual = table_items(table, PARTITION_KEY)
    problems = compare(expected, actual, time.time())
    report.problems += first_few(problems)
    if not problems:
        report.observed.append(
            f"{len(actual)} items for {len(expected)} windows: all complete, none repeated"
        )


def written_at_least(writer: WriterProcess, count: int) -> Callable[[], bool]:
    return lambda: writer.written >= count


def writer_drill(settings: DemoSettings, say: Say) -> DrillReport:
    report = DrillReport()
    recreate_topics(settings.bootstrap_servers, WRITER_TOPICS)
    table = clear_partition(settings)
    windows = synthetic_windows(60, time.time_ns() // 1_000_000)

    writer = WriterProcess(settings, f"chaos-writer-{uuid.uuid4()}")
    publisher = WindowPublisher(settings.bootstrap_servers, windows, every_s=0.2)
    writer.start()
    publisher.start()
    try:
        # Kill right after a write: by then the writer has read the first row
        # of the next window, and may or may not have committed the last one.
        for after in (10, 25, 40):
            wait_until(
                f"The writer did not write {after} windows",
                written_at_least(writer, after),
                timeout_s=120,
            )
            writer.kill()
            say(f"Killed the writer with SIGKILL after {writer.written} writes")
            writer.start()

        publisher.join(timeout=120)
        wait_until(
            "The writer did not write every window",
            lambda: len(table_items(table, PARTITION_KEY)) >= len(windows),
            timeout_s=180,
        )
    finally:
        writer.stop()

    report.observed.append(
        f"killed 3 times mid-stream; {writer.written} writes for {len(windows)} "
        f"windows, so {writer.written - len(windows)} written again after a kill"
    )
    check_table(settings, table, report)
    return report


def dynamodb_drill(settings: DemoSettings, say: Say) -> DrillReport:
    report = DrillReport()
    recreate_topics(settings.bootstrap_servers, WRITER_TOPICS)
    table = clear_partition(settings)
    windows = synthetic_windows(60, time.time_ns() // 1_000_000)

    group_id = f"chaos-writer-{uuid.uuid4()}"
    writer = WriterProcess(settings, group_id)
    publisher = WindowPublisher(settings.bootstrap_servers, windows, every_s=1.0)
    writer.start()
    publisher.start()
    try:
        wait_until(
            "The writer did not write 10 windows",
            lambda: writer.written >= 10,
            timeout_s=120,
        )
        say("Stopping DynamoDB Local for 30 s")
        try:
            compose("stop", "dynamodb-local")
            stopped_at = time.monotonic()
            time.sleep(5)  # let a write that was already under way finish
            committed_early = committed_offset(
                settings.bootstrap_servers, group_id, TRENDING_TOPIC
            )
            written_early = writer.written
            time.sleep(25)
            committed_late = committed_offset(
                settings.bootstrap_servers, group_id, TRENDING_TOPIC
            )
        finally:
            compose("start", "dynamodb-local")
        outage_s = time.monotonic() - stopped_at
        say("DynamoDB Local started again")

        report.observed.append(
            f"outage {outage_s:.0f} s: {writer.unreachable} backed-off retries, "
            f"committed offset {committed_early} -> {committed_late}, "
            f"{writer.written - written_early} writes"
        )
        if committed_late != committed_early or writer.written != written_early:
            report.problems.append("the writer wrote or committed during the outage")
        if writer.unreachable < 2:
            report.problems.append("the writer did not retry while DynamoDB was down")

        publisher.join(timeout=120)
        caught_up_from = time.monotonic()
        wait_until(
            "The writer did not catch up",
            lambda: len(table_items(table, PARTITION_KEY)) >= len(windows),
            timeout_s=180,
        )
        report.observed.append(
            f"caught up {time.monotonic() - caught_up_from:.0f} s after the last window"
        )
    finally:
        writer.stop()

    check_table(settings, table, report)
    return report


def scan_all(table: Any) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    arguments: dict[str, Any] = {}
    while True:
        response = table.scan(**arguments)
        items += [plain(item) for item in response["Items"]]
        if "LastEvaluatedKey" not in response:
            return items
        arguments["ExclusiveStartKey"] = response["LastEvaluatedKey"]


def continue_watching_drill(settings: DemoSettings, say: Say) -> DrillReport:
    report = DrillReport()
    recreate_topics(settings.bootstrap_servers, CW_TOPICS)
    dynamodb = dynamodb_resource(settings.dynamodb_endpoint)
    recreate_table(
        dynamodb.meta.client, load_table_definition(TABLE_DEFINITION), CW_TABLE
    )
    table = dynamodb.Table(CW_TABLE)

    writer = ContinueWatchingWriterProcess(settings, f"chaos-cw-writer-{uuid.uuid4()}")
    job_id = submit_job(CW_JOB_ARGS, script="continue_watching.py")
    try:
        wait_until(
            "The continue-watching job did not start running",
            lambda: job_is_running(settings.flink_url, job_id),
            timeout_s=180,
        )
        writer.start()
        producer = EventProducer(settings, CW_EVENTS_TOPIC)
        producer.start()
        say(
            f"Job and writer running; producing {RATE * DURATION_S:.0f} events "
            f"over {DURATION_S:.0f} s"
        )

        # Break things only once a checkpoint exists and changes are committed.
        wait_until(
            "No checkpoint completed with changes committed",
            lambda: (
                checkpoint_counts(settings.flink_url, job_id)["completed"] >= 1
                and end_offset(settings.bootstrap_servers, CW_TOPIC) > 0
            ),
            timeout_s=180,
        )
        report.observed += kill_taskmanager(settings, say, job_id).observed

        # Then kill the writer, once it has written again after the restore.
        writes = writer.written
        wait_until(
            "The writer did not write after the TaskManager came back",
            written_at_least(writer, writes + 3),
            timeout_s=180,
        )
        writer.kill()
        say(f"Killed the writer with SIGKILL after {writer.written} batches")
        writer.start()
        report.observed.append(
            f"writer killed after {writer.written} batches and started again at once"
        )

        producer.join(timeout=DURATION_S + 300)
        if producer.stats is None:
            raise DemoError("The producer did not finish")

        say("Waiting for the job to commit its last changes")
        wait_until_quiet(settings.bootstrap_servers, CW_TOPIC)

        events = generate_events(int(RATE * DURATION_S), seed=SEED, dup_rate=DUP_RATE)
        expected = oracle_programmes(continue_watching_oracle(events))
        # The writer may still be catching up, so give it two minutes.
        deadline = time.monotonic() + 120
        while True:
            problems = compare_listings(listed_programmes(scan_all(table)), expected)
            if not problems or time.monotonic() > deadline:
                break
            time.sleep(5)

        report.problems += first_few(problems)
        if not problems:
            users = len({user_id for user_id, _ in expected})
            watched = len({(e["user_id"], e["programme_id"]) for e in events})
            report.observed.append(
                f"{len(expected)} in-progress programmes for {users} users match the "
                f"oracle; the {watched - len(expected)} finished ones are not listed"
            )
    finally:
        writer.stop()
        try:
            stop_job(settings.flink, job_id)
        except (OSError, DemoError) as exc:
            say(f"Could not stop the continue-watching job {job_id}: {exc}")

    return report


def schema_drill(settings: DemoSettings, say: Say) -> DrillReport:
    """Producers move from v1 to v2 mid-stream while the job, reading v1, runs on."""
    events = len(generate_events(int(RATE * DURATION_S), seed=SEED, dup_rate=DUP_RATE))

    def upgrade_producers(job_id: str) -> DrillReport:
        report = DrillReport()
        registry = settings.schema_registry
        ids = [registry.register(subject(), schema_text(v)) for v in (1, 2)]
        say("The producer writes v1, then v2 from halfway; the job is left alone")
        wait_until(
            "The producer did not write its v2 half",
            lambda: end_offset(settings.bootstrap_servers, EVENTS_TOPIC) >= events,
            timeout_s=DURATION_S + 300,
        )
        restored = checkpoint_counts(settings.flink_url, job_id)["restored"]
        report.observed.append(
            f"the first {(events + 1) // 2} events written as v1 (schema id "
            f"{ids[0]}) and the other {events // 2} as v2 (id {ids[1]})"
        )
        if restored:
            report.problems.append(
                f"the job restarted {restored} time(s) while producers moved to v2"
            )
        else:
            report.observed.append("the job read both without restarting")
        return report

    report = run_job_drill(settings, say, upgrade_producers, schema_versions=(1, 2))
    dead_letters = end_offset(settings.bootstrap_servers, EVENTS_DLQ_TOPIC)
    if dead_letters:
        report.problems.append(f"{dead_letters} events were dead-lettered")
    else:
        report.observed.append("nothing dead-lettered")
    return report


DRILLS: dict[str, tuple[str, Callable[[DemoSettings, Say], DrillReport]]] = {
    "taskmanager": ("TaskManager killed mid-stream", taskmanager_drill),
    "kafka": ("Kafka broker restarted mid-stream", kafka_drill),
    "writer": ("Writer killed mid-batch", writer_drill),
    "dynamodb": ("DynamoDB unreachable", dynamodb_drill),
    "continue-watching": (
        "Continue watching: TaskManager and writer killed",
        continue_watching_drill,
    ),
    "schema": ("Producers move to schema v2 mid-stream", schema_drill),
}


def run_drills(
    settings: DemoSettings, names: Sequence[str], say: Say = print
) -> list[DrillResult]:
    say("Waiting for Kafka, Flink and DynamoDB Local")
    wait_for_services(settings)
    create_table_if_missing(
        dynamodb_resource(settings.dynamodb_endpoint).meta.client,
        load_table_definition(TABLE_DEFINITION),
    )

    results = []
    for name in names:
        title, drill = DRILLS[name]
        started = time.monotonic()

        def log(message: str, started: float = started, name: str = name) -> None:
            say(f"[{name} {time.monotonic() - started:4.0f}s] {message}")

        log(title)
        try:
            report = drill(settings, log)
        except (
            DemoError,
            OSError,
            KafkaException,
            BotoCoreError,
            ClientError,
            subprocess.CalledProcessError,
        ) as exc:
            report = DrillReport(problems=[f"the drill stopped: {exc}"])

        result = DrillResult(name, title, report, time.monotonic() - started)
        log(
            ("PASS" if result.passed else "FAIL")
            + ": "
            + "; ".join(report.observed + report.problems)
        )
        results.append(result)

    return results


def machine_summary() -> str:
    def output(*command: str) -> str:
        result = subprocess.run(
            command, check=False, cwd=REPO_ROOT, capture_output=True, text=True
        )
        return result.stdout.strip()

    commit = output("git", "rev-parse", "--short", "HEAD") or "unknown"
    if output("git", "status", "--porcelain"):
        commit += " plus local changes"
    cpus, memory = (
        output("docker", "info", "--format", "{{.NCPU}} {{.MemTotal}}") + " 0 0"
    ).split()[:2]
    when = datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC")

    return (
        f"on {when}, commit `{commit}`, Docker VM with {cpus} CPUs and "
        f"{int(memory) / 2**30:.1f} GB"
    )


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--drill", action="append", choices=list(DRILLS))
    args = parser.parse_args(argv)

    names = args.drill or list(DRILLS)
    settings = DemoSettings.from_env()

    try:
        results = run_drills(settings, names)
    except DemoError as exc:
        print(f"Chaos drills failed to start: {exc}", file=sys.stderr)
        sys.exit(1)

    if set(names) == set(DRILLS):
        record_results(RESULTS_DOC, render_results(results, machine_summary()))
        print(f"Recorded the results in {RESULTS_DOC.relative_to(REPO_ROOT)}")
    else:
        print("Ran a subset of the drills, so docs/reliability.md was not updated")

    sys.exit(0 if all(result.passed for result in results) else 1)


if __name__ == "__main__":
    main()
