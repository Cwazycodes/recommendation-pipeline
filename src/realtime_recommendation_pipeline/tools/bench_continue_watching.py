"""Benchmark the continue-watching job: a live run at each rate (make cw-bench).

Each run is set up like a trending run in make bench: a fresh TaskManager,
empty topics and a table of its own, a new job and writer, and live events at
one rate for a fixed time. Throughput, consumer lag and each Flink task's busy
time come from Prometheus, as for trending. Update latency comes from the
writer, which records, for every change it stores, the time since its event
was produced. Results go to docs/benchmarks/continue-watching/, and
docs/benchmarks.md explains them.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
import threading
import time
import uuid
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from pathlib import Path

from confluent_kafka import Consumer, Producer

from realtime_recommendation_pipeline.common.trending_table import (
    TIME_FORMAT,
    dynamodb_resource,
    load_table_definition,
    recreate_table,
)
from realtime_recommendation_pipeline.producer.kafka_producer import produce_events
from realtime_recommendation_pipeline.tools.bench import (
    PERCENTILES,
    SCRAPE_WAIT_S,
    SETTLE_S,
    WARMUP_S,
    Row,
    Run,
    TaskTime,
    as_text,
    cell,
    end_job,
    fresh_taskmanager,
    machine_specs,
    merge,
    number,
    read_rows,
    read_task_times,
    show,
    task_rows,
    write_rows,
)
from realtime_recommendation_pipeline.tools.demo import (
    REPO_ROOT,
    TABLE_DEFINITION,
    DemoError,
    DemoSettings,
    job_is_running,
    recreate_topics,
    submit_job,
    wait_for_services,
    wait_until,
)
from realtime_recommendation_pipeline.tools.metrics_report import (
    CONTINUE_WATCHING_SOURCE,
    QUANTILES,
    Ingest,
    MetricsError,
    prometheus_query,
    prometheus_query_by_label,
    read_ingest,
)
from realtime_recommendation_pipeline.writer.continue_watching_writer import (
    ContinueWatchingWriter,
    WriteSamples,
)
from realtime_recommendation_pipeline.writer.results_writer import (
    NO_SDK_RETRIES,
    KafkaDeadLetters,
    writer_consumer_config,
)

OUT_DIR = REPO_ROOT / "docs" / "benchmarks" / "continue-watching"
EVENTS_TOPIC = "viewing-events-cw-bench"
CW_TOPIC = "continue-watching-bench"
CW_DLQ_TOPIC = "continue-watching-writer-dlq-bench"
TABLE = "continue-watching-bench"

RESULT_COLUMNS = [
    "parallelism", "partitions", "rate", "duration_s", "seed", "started_at",
    "produced_per_s", "read_per_s",
    "lag_end", "lag_max", "lag_slope_per_s", "lag_trend", "items_written",
    *(f"latency_{p}_s" for p in PERCENTILES),
    *(f"batch_write_{p}_ms" for p in PERCENTILES),
    "busiest_task", "busiest_task_busy_ms_per_s",
]  # fmt: skip
TASK_COLUMNS = [
    "parallelism", "partitions", "rate", "task", "busy_ms_per_s",
    "backpressured_ms_per_s",
]  # fmt: skip


def percentile(values: Sequence[float], q: float) -> float | None:
    """Nearest-rank percentile: the smallest value at or above q of them."""
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(math.ceil(q * len(ordered)) - 1, 0)]


def between(
    samples: Sequence[tuple[float, float]], start: float, end: float
) -> list[float]:
    return [value for at, value in samples if start <= at <= end]


class WriterThread(threading.Thread):
    """The continue-watching writer on the benchmark's topic and table."""

    def __init__(self, settings: DemoSettings) -> None:
        super().__init__(name="continue-watching-writer", daemon=True)
        self.settings = settings
        self.stopping = threading.Event()
        self.samples = WriteSamples()

    def run(self) -> None:
        # A new group each run, so no offsets are left over from earlier runs.
        group_id = f"continue-watching-writer-bench-{uuid.uuid4()}"
        consumer = Consumer(
            writer_consumer_config(self.settings.bootstrap_servers, group_id, group_id)
        )
        producer = Producer(
            {
                "bootstrap.servers": self.settings.bootstrap_servers,
                "enable.idempotence": True,
                "acks": "all",
            }
        )
        dynamodb = dynamodb_resource(self.settings.dynamodb_endpoint, NO_SDK_RETRIES)
        writer = ContinueWatchingWriter(
            consumer,
            dynamodb.Table(TABLE),
            KafkaDeadLetters(producer, CW_DLQ_TOPIC, key_field=None),
            samples=self.samples,
        )
        consumer.subscribe([CW_TOPIC])
        try:
            writer.run(self.stopping.is_set)
        finally:
            consumer.close()


def result_row(
    run: Run,
    started_at: float,
    ingest: Ingest,
    latency_s: Sequence[float],
    write_s: Sequence[float],
    tasks: dict[str, TaskTime],
) -> Row:
    busiest = max(tasks, key=lambda task: tasks[task].busy_ms_per_s or 0, default="")
    latency = {
        f"latency_{p}_s": cell(percentile(latency_s, q))
        for p, q in zip(PERCENTILES, QUANTILES, strict=True)
    }
    write = {
        f"batch_write_{p}_ms": cell(
            None if (s := percentile(write_s, q)) is None else s * 1000
        )
        for p, q in zip(PERCENTILES, QUANTILES, strict=True)
    }

    return {
        "parallelism": str(run.parallelism),
        "partitions": str(run.partitions),
        "rate": cell(run.rate),
        "duration_s": cell(run.duration_s),
        "seed": str(run.seed),
        "started_at": datetime.fromtimestamp(started_at, UTC).strftime(TIME_FORMAT),
        "produced_per_s": cell(ingest.produced_per_s),
        "read_per_s": cell(ingest.events_per_s),
        "lag_end": cell(ingest.lag_end),
        "lag_max": cell(ingest.lag_max),
        "lag_slope_per_s": cell(ingest.lag_slope_per_s),
        "lag_trend": ingest.lag_trend,
        "items_written": str(len(latency_s)),
        **latency,
        **write,
        "busiest_task": busiest,
        "busiest_task_busy_ms_per_s": (
            cell(tasks[busiest].busy_ms_per_s) if busiest else ""
        ),
    }


def bench_run(
    settings: DemoSettings, run: Run, say: Callable[[str], None]
) -> tuple[Row, list[Row]]:
    """One run on a fresh TaskManager, topics and table; its rows."""
    fresh_taskmanager(settings, run.parallelism)
    recreate_topics(
        settings.bootstrap_servers,
        {EVENTS_TOPIC: run.partitions, CW_TOPIC: 1, CW_DLQ_TOPIC: 1},
    )
    dynamodb = dynamodb_resource(settings.dynamodb_endpoint)
    recreate_table(dynamodb.meta.client, load_table_definition(TABLE_DEFINITION), TABLE)

    job_id = submit_job(
        [
            "--source-topic", EVENTS_TOPIC,
            "--continue-watching-topic", CW_TOPIC,
            "--parallelism", str(run.parallelism),
        ],
        script="continue_watching.py",
    )  # fmt: skip
    writer = WriterThread(settings)
    try:
        wait_until(
            "The continue-watching job did not start running",
            lambda: job_is_running(settings.flink_url, job_id),
            timeout_s=120,
        )
        writer.start()
        stats = produce_events(
            bootstrap_servers=settings.bootstrap_servers,
            rate=run.rate,
            duration=run.duration_s,
            seed=run.seed,
            topic=EVENTS_TOPIC,
            registry=settings.schema_registry,
            live=True,
        )
        sent_from, sent_to = stats["sent_from_ms"] / 1000, stats["sent_to_ms"] / 1000
        say(
            f"  Sent {stats['delivered']:,} events in {sent_to - sent_from:.0f} s; "
            f"waiting {SETTLE_S} s for the last changes"
        )
        time.sleep(SETTLE_S + SCRAPE_WAIT_S)
        if not writer.is_alive():
            raise DemoError("The continue-watching writer stopped; its error is above")

        url = settings.metrics.prometheus_url
        warm = sent_from + min(WARMUP_S, (sent_to - sent_from) / 2)
        ingest = read_ingest(
            prometheus_query(url), EVENTS_TOPIC, warm, sent_to, CONTINUE_WATCHING_SOURCE
        )
        tasks = read_task_times(
            prometheus_query_by_label(url, "task_name"), job_id, warm, sent_to
        )
        # Changes to events sent after the warm-up, written by the end of the wait.
        done = sent_to + SETTLE_S
        latency_s = between(writer.samples.latency_s, warm, done)
        write_s = between(writer.samples.write_s, warm, done)
    finally:
        writer.stopping.set()
        if writer.is_alive():
            writer.join(timeout=15)
        end_job(settings, job_id, say)

    row = result_row(run, sent_from, ingest, latency_s, write_s, tasks)
    return row, task_rows(run, tasks)


def markdown_table(results: Sequence[Row]) -> list[str]:
    """The results as a Markdown table for docs/benchmarks.md."""
    header = (
        "| Parallelism | Rate (events/s) | Read/s | Lag at end | Lag trend "
        "| Items written | Update latency p50 / p95 / p99 (s) "
        "| Batch write p50 / p95 / p99 (ms) | Busiest task (busy ms/s) |"
    )
    lines = [header, "|---:|---:|---:|---:|---|---:|---|---|---|"]
    for row in results:
        latency = " / ".join(
            as_text(number(row, f"latency_{p}_s"), 1) for p in PERCENTILES
        )
        write = " / ".join(
            as_text(number(row, f"batch_write_{p}_ms")) for p in PERCENTILES
        )
        lines.append(
            f"| {row['parallelism']} | {as_text(number(row, 'rate'))} "
            f"| {as_text(number(row, 'read_per_s'))} "
            f"| {as_text(number(row, 'lag_end'))} | {row['lag_trend']} "
            f"| {as_text(number(row, 'items_written'))} | {latency} | {write} "
            f"| {row['busiest_task'] or 'n/a'} "
            f"({as_text(number(row, 'busiest_task_busy_ms_per_s'))}) |"
        )
    return lines


def run_bench(
    settings: DemoSettings,
    runs: Sequence[Run],
    out_dir: Path = OUT_DIR,
    say: Callable[[str], None] = show,
) -> None:
    if settings.dynamodb_endpoint is None:
        raise DemoError(
            "make cw-bench runs on DynamoDB Local (DYNAMODB_ENDPOINT_URL): it "
            "creates a table of its own"
        )
    say("Waiting for Kafka, Flink and DynamoDB Local")
    wait_for_services(settings)
    out_dir.mkdir(parents=True, exist_ok=True)
    specs = machine_specs(settings.flink_url)
    (out_dir / "machine.json").write_text(json.dumps(specs, indent=2) + "\n")

    for index, run in enumerate(runs, start=1):
        say(
            f"Run {index}/{len(runs)}: {run.rate:,.0f} events/s for "
            f"{run.duration_s:.0f} s, parallelism {run.parallelism}, "
            f"{run.partitions} partitions, seed {run.seed}"
        )
        result, tasks = bench_run(settings, run, say)
        results_csv, tasks_csv = out_dir / "results.csv", out_dir / "tasks.csv"
        write_rows(results_csv, RESULT_COLUMNS, merge(read_rows(results_csv), [result]))
        write_rows(tasks_csv, TASK_COLUMNS, merge(read_rows(tasks_csv), tasks))
        say(
            f"  Read {as_text(number(result, 'read_per_s'))} events/s, lag "
            f"{result['lag_trend']}, update latency p95 "
            f"{as_text(number(result, 'latency_p95_s'), 1)} s, busiest task "
            f"{result['busiest_task'] or 'n/a'}"
        )


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rates", type=float, nargs="+", default=[])
    parser.add_argument("--duration", type=float, default=120.0)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--parallelism", type=int, default=1)
    parser.add_argument("--partitions", type=int, default=6)
    parser.add_argument("--out-dir", type=Path, default=OUT_DIR)
    parser.add_argument(
        "--report-only",
        action="store_true",
        help="print the table from the saved results",
    )
    args = parser.parse_args(argv)
    out_dir = args.out_dir.resolve()

    if not args.report_only:
        settings = DemoSettings.from_env()
        runs = [
            Run(args.parallelism, args.partitions, rate, args.duration, args.seed)
            for rate in args.rates
        ]
        try:
            run_bench(settings, runs, out_dir)
        except (DemoError, MetricsError, OSError) as exc:
            print(f"Benchmark failed: {exc}", file=sys.stderr)
            sys.exit(1)
        except KeyboardInterrupt:
            sys.exit(130)

    for line in markdown_table(read_rows(out_dir / "results.csv")):
        show(line)


if __name__ == "__main__":
    main()
