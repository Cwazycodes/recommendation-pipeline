"""Benchmark the pipeline: a fixed-duration live run at each rate, one at a time.

Each run is the demo's job, topics and results writer, with live events at one
rate for the same duration and seed. When it ends, throughput, consumer lag,
latency percentiles (docs/metrics.md) and each Flink task's busy time are read
from Prometheus and saved in docs/benchmarks/, with the machine's specs and
the charts. docs/benchmarks.md explains the results.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import platform
import re
import subprocess
import sys
import time
import urllib.request
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from matplotlib.axes import Axes
from matplotlib.figure import Figure
from matplotlib.ticker import FuncFormatter

from realtime_recommendation_pipeline.common.trending_table import (
    TIME_FORMAT,
    create_table_if_missing,
    dynamodb_resource,
    load_table_definition,
)
from realtime_recommendation_pipeline.producer.kafka_producer import produce_events
from realtime_recommendation_pipeline.tools.demo import (
    DEMO_JOB_ARGS,
    EVENTS_TOPIC,
    REPO_ROOT,
    TABLE_DEFINITION,
    TOPICS,
    DemoError,
    DemoSettings,
    WriterThread,
    flink_request,
    job_is_running,
    recreate_topics,
    stop_job,
    submit_job,
    wait_for_services,
    wait_until,
)
from realtime_recommendation_pipeline.tools.metrics_report import (
    QUANTILES,
    Ingest,
    Latency,
    MetricsError,
    QueryByLabel,
    lookback,
    prometheus_query,
    prometheus_query_by_label,
    read_ingest,
    read_latency,
)

OUT_DIR = REPO_ROOT / "docs" / "benchmarks"

# Each run starts a new job, whose Python workers start cold, so throughput,
# lag and busy time leave out the first 20 s of sending.
WARMUP_S = 20
# After producing stops, the windows that closed during the run still wait for
# the checkpoint that commits them (up to 10 s) and the writer's 2 s flush.
SETTLE_S = 30
# Prometheus scrapes every 5 s, so wait for the samples at the end of a run.
SCRAPE_WAIT_S = 10

TASK_METRIC = "flink_taskmanager_job_task"
FINAL_STATES = ("FINISHED", "CANCELED", "FAILED")
PERCENTILES = [f"p{round(q * 100)}" for q in QUANTILES]
RESULT_COLUMNS = [
    "parallelism", "partitions", "rate", "duration_s", "seed", "started_at",
    "produced_per_s", "read_per_s",
    "lag_end", "lag_max", "lag_slope_per_s", "lag_trend",
    "watermark_delay_s", "windows",
    *(f"{kind}_{p}_s" for kind in ("freshness", "processing", "delivery")
      for p in PERCENTILES),
      *(f"write_{p}_ms" for p in PERCENTILES),
    "busiest_task", "busiest_task_busy_ms_per_s",
]  # fmt: skip
TASK_COLUMNS = [
    "parallelism", "partitions", "rate", "task", "busy_ms_per_s",
    "backpressured_ms_per_s",
]  # fmt: skip
# design.md section 6: p95 processing latency of 30 s or less.
LATENCY_TARGET_S = 30
# A run is one rate on one setup; running it again replaces its rows.
RUN_KEY = ("parallelism", "partitions", "rate")

Row = dict[str, str]


@dataclass(frozen=True)
class Run:
    """One benchmark run: the setup, and the load it is given."""

    parallelism: int
    partitions: int
    rate: float
    duration_s: float
    seed: int


@dataclass(frozen=True)
class TaskTime:
    """A Flink task's average ms per second busy and back-pressured in a run."""

    busy_ms_per_s: float | None
    backpressured_ms_per_s: float | None


def task_label(task_name: str) -> str:
    """A task by its first operator: viewing-events-source, WindowRank, ...

    Flink's reporter writes " -> " between chained operators as "____", ", "
    as "__" and other punctuation as "_".
    """
    first = task_name.split("____")[0].removeprefix("Source:_")
    first = re.sub(r"_\d+_?$", "", first)
    return first.replace("__", ", ").replace("_", "-")


def read_task_times(
    query: QueryByLabel, job_id: str, start: float, end: float
) -> dict[str, TaskTime]:
    """Each task's time over the run, from its busiest subtask."""
    span = lookback(end - start)

    def per_task(metric: str) -> dict[str, float]:
        selector = f'{TASK_METRIC}_{metric}{{job_id="{job_id}"}}'
        expression = f"max by (task_name) (avg_over_time({selector}[{span}]))"
        return {
            task_label(name): value for name, value in query(expression, end).items()
        }

    busy = per_task("busyTimeMsPerSecond")
    backpressured = per_task("backPressuredTimeMsPerSecond")
    return {
        task: TaskTime(busy.get(task), backpressured.get(task))
        for task in sorted(busy.keys() | backpressured.keys())
    }


def cell(value: float | None) -> str:
    """A CSV cell: at most 2 decimals, empty when there is no value."""
    if value is None:
        return ""
    text = f"{value:.2f}".rstrip("0").rstrip(".")
    return "0" if text == "-0" else text


def result_row(
    run: Run,
    started_at: float,
    ingest: Ingest,
    latency: Latency,
    tasks: Mapping[str, TaskTime],
) -> Row:
    busiest = max(tasks, key=lambda task: tasks[task].busy_ms_per_s or 0, default="")
    percentiles = {
        f"{kind}_{p}_s": cell(values[q])
        for kind, values in (
            ("freshness", latency.freshness),
            ("processing", latency.processing),
            ("delivery", latency.delivery),
        )
        for p, q in zip(PERCENTILES, QUANTILES, strict=True)
    }
    # Milliseconds: a put takes a few of them, below what 2 decimals of a
    # second show.
    write_ms = {q: None if s is None else s * 1000 for q, s in latency.write.items()}
    write = {
        f"write_{p}_ms": cell(write_ms.get(q))
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
        "watermark_delay_s": cell(ingest.watermark_delay_s),
        "windows": cell(None if latency.windows is None else round(latency.windows)),
        **percentiles,
        **write,
        "busiest_task": busiest,
        "busiest_task_busy_ms_per_s": (
            cell(tasks[busiest].busy_ms_per_s) if busiest else ""
        ),
    }


def task_rows(run: Run, tasks: Mapping[str, TaskTime]) -> list[Row]:
    return [
        {
            "parallelism": str(run.parallelism),
            "partitions": str(run.partitions),
            "rate": cell(run.rate),
            "task": task,
            "busy_ms_per_s": cell(spent.busy_ms_per_s),
            "backpressured_ms_per_s": cell(spent.backpressured_ms_per_s),
        }
        for task, spent in tasks.items()
    ]


# --- Files in docs/benchmarks/ ------------------------------------------------


def run_key(row: Mapping[str, str]) -> tuple[float, ...]:
    return tuple(float(row[column]) for column in RUN_KEY)


def merge(saved: Sequence[Row], new: Sequence[Row]) -> list[Row]:
    """The saved rows with these runs' rows in place of any from the same runs."""
    redone = {run_key(row) for row in new}
    kept = [row for row in saved if run_key(row) not in redone]
    return sorted([*kept, *new], key=run_key)


def read_rows(path: Path) -> list[Row]:
    if not path.exists():
        return []
    with path.open(newline="") as file:
        return list(csv.DictReader(file))


def write_rows(path: Path, columns: Sequence[str], rows: Sequence[Row]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


def save_run(out_dir: Path, result: Row, tasks: Sequence[Row]) -> None:
    """Add a run to results.csv and tasks.csv, replacing an earlier one."""
    results_csv, tasks_csv = out_dir / "results.csv", out_dir / "tasks.csv"
    write_rows(results_csv, RESULT_COLUMNS, merge(read_rows(results_csv), [result]))
    write_rows(tasks_csv, TASK_COLUMNS, merge(read_rows(tasks_csv), tasks))


def command_output(command: Sequence[str]) -> str | None:
    """What the command prints, or None if it fails or is not installed."""
    try:
        result = subprocess.run(
            command, capture_output=True, text=True, timeout=30, check=True
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return result.stdout.strip()


def cpu_model() -> str:
    if sys.platform == "darwin":
        brand = command_output(["sysctl", "-n", "machdep.cpu.brand_string"])
        return brand or platform.processor()
    try:
        for line in Path("/proc/cpuinfo").read_text().splitlines():
            if line.startswith("model name"):
                return line.split(":", 1)[1].strip()
    except OSError:
        pass
    return platform.processor()


def gib(size_bytes: float) -> float:
    return round(size_bytes / 1024**3, 1)


def machine_specs(flink_url: str) -> dict[str, Any]:
    """The host, the Docker VM and the Flink cluster the runs were measured on."""
    docker = json.loads(
        command_output(["docker", "info", "--format", "{{json .}}"]) or "{}"
    )
    overview = flink_request(f"{flink_url}/overview")
    taskmanagers = flink_request(f"{flink_url}/taskmanagers")["taskmanagers"]
    memory = taskmanagers[0]["memoryConfiguration"] if taskmanagers else {}

    return {
        "recorded_at": datetime.now(UTC).strftime(TIME_FORMAT),
        "host": {
            "os": platform.platform(),
            "cpu": cpu_model(),
            "logical_cpus": os.cpu_count(),
            "memory_gib": gib(os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")),
            "python": platform.python_version(),
        },
        "docker": {
            "cpus": docker.get("NCPU"),
            "memory_gib": gib(docker.get("MemTotal", 0)),
            "engine": docker.get("ServerVersion"),
            "os": docker.get("OperatingSystem"),
        },
        "flink": {
            "version": overview["flink-version"],
            "taskmanagers": overview["taskmanagers"],
            "task_slots": overview["slots-total"],
            "taskmanager_memory_gib": gib(memory.get("totalProcessMemory", 0)),
            "managed_memory_gib": gib(memory.get("managedMemory", 0)),
        },
    }


# --- Reading results back -----------------------------------------------------


def number(row: Mapping[str, str], column: str) -> float | None:
    return float(row[column]) if row.get(column) else None


def setup_of(row: Mapping[str, str]) -> tuple[int, int]:
    return int(row["parallelism"]), int(row["partitions"])


def setup_name(setup: tuple[int, int]) -> str:
    return f"parallelism {setup[0]}, {setup[1]} partitions"


def by_setup(rows: Sequence[Row]) -> dict[tuple[int, int], list[Row]]:
    setups: dict[tuple[int, int], list[Row]] = {}
    for row in rows:
        setups.setdefault(setup_of(row), []).append(row)
    return dict(sorted(setups.items()))


def kept_up(row: Mapping[str, str]) -> bool:
    return row["lag_trend"] in ("steady", "shrinking")


@dataclass(frozen=True)
class Summary:
    """A setup's breaking point and the throughput it sustained below it."""

    setup: tuple[int, int]
    breaking_rate: float | None
    sustained_rate: float | None
    sustained_read_per_s: float | None


def summarise(results: Sequence[Row]) -> list[Summary]:
    """Breaking point: the lowest rate whose lag grew. Sustained: the highest
    rate below it whose lag did not grow (docs/metrics.md, "Throughput")."""
    summaries = []
    for setup, rows in by_setup(results).items():
        grew = [float(row["rate"]) for row in rows if row["lag_trend"] == "growing"]
        breaking = min(grew, default=None)
        below = [
            row
            for row in rows
            if kept_up(row) and (breaking is None or float(row["rate"]) < breaking)
        ]
        best = max(below, key=lambda row: float(row["rate"]), default=None)
        summaries.append(
            Summary(
                setup,
                breaking,
                float(best["rate"]) if best else None,
                number(best, "read_per_s") if best else None,
            )
        )
    return summaries


def as_text(value: float | None, digits: int = 0) -> str:
    return "n/a" if value is None else f"{value:,.{digits}f}"


def markdown_table(results: Sequence[Row]) -> list[str]:
    """The results as a Markdown table for docs/benchmarks.md, and a summary."""
    header = (
        "| Parallelism | Partitions | Rate (events/s) | Produced/s | Read/s "
        "| Lag at end | Lag trend | Windows | Processing p50 / p95 / p99 (s) "
        "| DynamoDB put p50 / p95 / p99 (ms) | Busiest task (busy ms/s) |"
    )
    lines = [header, "|---:|---:|---:|---:|---:|---:|---|---:|---|---|---|"]
    for row in results:
        processing = " / ".join(
            as_text(number(row, f"processing_{p}_s"), 1) for p in PERCENTILES
        )
        put = " / ".join(as_text(number(row, f"write_{p}_ms")) for p in PERCENTILES)
        busiest = row["busiest_task"] or "n/a"
        busy = as_text(number(row, "busiest_task_busy_ms_per_s"))
        lines.append(
            f"| {row['parallelism']} | {row['partitions']} "
            f"| {as_text(number(row, 'rate'))} | {as_text(number(row, 'produced_per_s'))} "
            f"| {as_text(number(row, 'read_per_s'))} | {as_text(number(row, 'lag_end'))} "
            f"| {row['lag_trend']} | {as_text(number(row, 'windows'))} | {processing} "
            f"| {put} | {busiest} ({busy}) |"
        )

    lines.append("")
    for summary in summarise(results):
        breaking = (
            f"lag grew from {as_text(summary.breaking_rate)} events/s"
            if summary.breaking_rate is not None
            else "lag never grew"
        )
        sustained = (
            f"sustained {as_text(summary.sustained_read_per_s)} events/s "
            f"(the {as_text(summary.sustained_rate)} events/s run)"
            if summary.sustained_rate is not None
            else "no run below the breaking point"
        )
        lines.append(f"- {setup_name(summary.setup)}: {sustained}; {breaking}.")
    return lines


# --- Charts -------------------------------------------------------------------


def plot(
    axes: Axes, points: Sequence[tuple[float, float]], style: str, **options: Any
) -> None:
    """Draw (rate, value) points in rate order; nothing if there are none."""
    if points:
        rates, values = zip(*sorted(points), strict=True)
        axes.plot(rates, values, style, **options)


PLAIN = FuncFormatter(lambda value, _position: f"{value:,.0f}")


def rate_axis(axes: Axes, rows: Sequence[Row]) -> None:
    """A log axis of producer rates, with a tick at each rate measured."""
    axes.set_xscale("log")
    axes.set_xticks(sorted({float(row["rate"]) for row in rows}))
    axes.xaxis.set_major_formatter(PLAIN)
    axes.xaxis.set_minor_formatter("")
    axes.set_xlabel("Producer rate (events/s)")
    axes.grid(True, which="both", alpha=0.3)


def throughput_chart(results: Sequence[Row], path: Path) -> None:
    figure = Figure(figsize=(8, 5), layout="constrained")
    axes = figure.subplots()
    rates = sorted({float(row["rate"]) for row in results})
    axes.plot(rates, rates, "--", color="grey", label="read = rate")
    for index, (setup, rows) in enumerate(by_setup(results).items()):
        read = [
            (float(row["rate"]), value, row["lag_trend"])
            for row in rows
            if (value := number(row, "read_per_s")) is not None
        ]
        plot(axes, [(rate, value) for rate, value, _ in read], "o-",
             color=f"C{index}", label=setup_name(setup))  # fmt: skip
        grew = [(rate, value) for rate, value, trend in read if trend == "growing"]
        plot(axes, grew, "x", color="black", markersize=10)
    axes.plot([], [], "x", color="black", markersize=10, label="lag growing")
    rate_axis(axes, results)
    axes.set_yscale("log")
    axes.yaxis.set_major_formatter(PLAIN)
    axes.set_ylabel("Events read by the job (events/s)")
    axes.set_title("Throughput: events read per second at each producer rate")
    axes.legend()
    figure.savefig(path, dpi=120)


def latency_chart(results: Sequence[Row], path: Path) -> None:
    figure = Figure(figsize=(8, 5), layout="constrained")
    axes = figure.subplots()
    for index, (setup, rows) in enumerate(by_setup(results).items()):
        for p, style in zip(PERCENTILES, ("o:", "o-", "s--"), strict=True):
            points = [
                (float(row["rate"]), value)
                for row in rows
                if (value := number(row, f"processing_{p}_s")) is not None
            ]
            plot(axes, points, style, color=f"C{index}",
                 label=f"{setup_name(setup)}, {p}")  # fmt: skip
    axes.axhline(LATENCY_TARGET_S, color="grey", linestyle="--",
                 label=f"target: p95 {LATENCY_TARGET_S} s")  # fmt: skip
    rate_axis(axes, results)
    axes.set_yscale("log")
    axes.yaxis.set_major_formatter(PLAIN)
    axes.set_ylabel("Processing latency (s): freshness - 30 s")
    axes.set_title("Latency of the windows written in each run")
    axes.legend(fontsize="small")
    figure.savefig(path, dpi=120)


def busy_chart(tasks: Sequence[Row], path: Path) -> None:
    setups = by_setup(tasks)
    figure = Figure(figsize=(6 * len(setups), 5), layout="constrained")
    panels = figure.subplots(1, len(setups), squeeze=False)[0]
    for axes, (setup, rows) in zip(panels, setups.items(), strict=True):
        for task in sorted({row["task"] for row in rows}):
            points = [
                (float(row["rate"]), value)
                for row in rows
                if row["task"] == task
                and (value := number(row, "busy_ms_per_s")) is not None
            ]
            plot(axes, points, "o-", label=task)
        axes.axhline(1000, color="grey", linestyle="--")
        rate_axis(axes, rows)
        axes.set_ylim(0, 1050)
        axes.set_ylabel("Busy time of the busiest subtask (ms/s)")
        axes.set_title(f"Flink tasks, {setup_name(setup)}")
        axes.legend(fontsize="small")
    figure.savefig(path, dpi=120)


def draw_charts(out_dir: Path) -> list[Path]:
    """Charts from results.csv and tasks.csv, saved next to them."""
    results = read_rows(out_dir / "results.csv")
    tasks = read_rows(out_dir / "tasks.csv")
    charts: list[tuple[Callable[[Sequence[Row], Path], None], Sequence[Row], str]] = [
        (throughput_chart, results, "throughput.png"),
        (latency_chart, results, "latency.png"),
        (busy_chart, tasks, "busy.png"),
    ]
    paths = []
    for draw, rows, name in charts:
        if rows:
            draw(rows, out_dir / name)
            paths.append(out_dir / name)
    return paths


# --- Running ------------------------------------------------------------------


def show(line: str) -> None:
    """Print a line at once, also when the output goes to a file."""
    print(line, flush=True)


def cancel_job(flink_url: str, job_id: str) -> None:
    request = urllib.request.Request(
        f"{flink_url}/jobs/{job_id}?mode=cancel", method="PATCH"
    )
    urllib.request.urlopen(request, timeout=10).close()


def end_job(settings: DemoSettings, job_id: str, say: Callable[[str], None]) -> None:
    """Stop with a savepoint; if that fails (say, under heavy backpressure), cancel."""
    try:
        stop_job(settings.flink, job_id)
    except (OSError, DemoError) as exc:
        say(f"  Could not stop the job with a savepoint ({exc}); cancelling it")
        cancel_job(settings.flink_url, job_id)


def taskmanager_ids(flink_url: str) -> set[str]:
    return {
        tm["id"] for tm in flink_request(f"{flink_url}/taskmanagers")["taskmanagers"]
    }


def fresh_taskmanager(settings: DemoSettings, parallelism: int) -> None:
    """Restart the TaskManager and wait for its slots.

    Every run then starts the same way, without the memory that the last run's
    RocksDB and Python workers kept: on a 4 GB Docker VM it built up until a
    later run's Python workers could not start.
    """
    url = settings.flink_url
    jobs = flink_request(f"{url}/jobs/overview")["jobs"]
    active = [job["jid"] for job in jobs if job["state"] not in FINAL_STATES]
    if active:
        raise DemoError(
            f"Flink is running job {active[0]}; stop it first (make stop-job), "
            "so the benchmark has the TaskManager to itself"
        )
    slots = flink_request(f"{url}/overview")["slots-total"]
    if slots < parallelism:
        raise DemoError(
            f"Parallelism {parallelism} needs {parallelism} task slots; Flink has {slots}"
        )

    old = taskmanager_ids(url)
    result = subprocess.run(
        ["docker", "compose", "restart", "flink-taskmanager"],
        check=False,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    if result.returncode:
        raise DemoError(f"Could not restart the TaskManager:\n{result.stderr}")

    def ready() -> bool:
        ids = taskmanager_ids(url)
        free = flink_request(f"{url}/overview")["slots-available"]
        return bool(ids) and not ids & old and free >= parallelism

    wait_until("The restarted TaskManager did not register", ready, timeout_s=180)


def bench_run(
    settings: DemoSettings, run: Run, say: Callable[[str], None]
) -> tuple[Row, list[Row]]:
    """One run on a fresh TaskManager and demo topics, with a new job and
    writer; its rows."""
    fresh_taskmanager(settings, run.parallelism)
    recreate_topics(
        settings.bootstrap_servers, {**TOPICS, EVENTS_TOPIC: run.partitions}
    )
    job_id = submit_job([*DEMO_JOB_ARGS, "--parallelism", str(run.parallelism)])
    writer = WriterThread(settings)
    try:
        wait_until(
            "The Flink job did not start running",
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
            f"waiting {SETTLE_S} s for the last windows"
        )
        time.sleep(SETTLE_S + SCRAPE_WAIT_S)
        if not writer.is_alive():
            raise DemoError(
                "The results writer stopped; its error is above (is make writer "
                "or make demo running? They serve metrics on the same port)"
            )

        url = settings.metrics.prometheus_url
        warm = sent_from + min(WARMUP_S, (sent_to - sent_from) / 2)
        ingest = read_ingest(prometheus_query(url), EVENTS_TOPIC, warm, sent_to)
        latency = read_latency(prometheus_query(url), warm, sent_to + SETTLE_S)
        tasks = read_task_times(
            prometheus_query_by_label(url, "task_name"), job_id, warm, sent_to
        )
    finally:
        writer.stopping.set()
        if writer.is_alive():
            writer.join(timeout=15)
        end_job(settings, job_id, say)

    return result_row(run, sent_from, ingest, latency, tasks), task_rows(run, tasks)


def run_bench(
    settings: DemoSettings,
    runs: Sequence[Run],
    out_dir: Path = OUT_DIR,
    say: Callable[[str], None] = show,
) -> None:
    say("Waiting for Kafka, Flink and DynamoDB")
    wait_for_services(settings)
    dynamodb = dynamodb_resource(settings.dynamodb_endpoint)
    # In AWS, Terraform made the table (infra/terraform), and the writer's
    # role may only put items in it.
    if settings.dynamodb_endpoint:
        create_table_if_missing(
            dynamodb.meta.client, load_table_definition(TABLE_DEFINITION)
        )
    specs = machine_specs(settings.flink_url)
    specs["dynamodb"] = settings.dynamodb_endpoint or (
        f"AWS {dynamodb.meta.client.meta.region_name}"
    )
    (out_dir / "machine.json").parent.mkdir(parents=True, exist_ok=True)
    (out_dir / "machine.json").write_text(json.dumps(specs, indent=2) + "\n")

    minutes = sum(run.duration_s + SETTLE_S + 90 for run in runs) / 60
    say(f"{len(runs)} runs, about {minutes:.0f} minutes")
    for index, run in enumerate(runs, start=1):
        say(
            f"Run {index}/{len(runs)}: {run.rate:,.0f} events/s for "
            f"{run.duration_s:.0f} s, parallelism {run.parallelism}, "
            f"{run.partitions} partitions, seed {run.seed}"
        )
        result, tasks = bench_run(settings, run, say)
        save_run(out_dir, result, tasks)
        say(
            f"  Read {as_text(number(result, 'read_per_s'))} events/s, lag "
            f"{result['lag_trend']}, processing p95 "
            f"{as_text(number(result, 'processing_p95_s'), 1)} s, DynamoDB put p95 "
            f"{as_text(number(result, 'write_p95_ms'))} ms, busiest task "
            f"{result['busiest_task'] or 'n/a'}"
        )


def report(out_dir: Path = OUT_DIR, say: Callable[[str], None] = show) -> None:
    results = read_rows(out_dir / "results.csv")
    # The charts compare rates, so a single rate gets the table only.
    if len({row["rate"] for row in results}) > 1:
        for path in draw_charts(out_dir):
            say(f"Drew {path.relative_to(REPO_ROOT)}")
        say("")
    for line in markdown_table(results):
        say(line)


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rates", type=float, nargs="+", default=[])
    parser.add_argument("--duration", type=float, default=120.0)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--parallelism", type=int, default=1)
    parser.add_argument("--partitions", type=int, default=TOPICS[EVENTS_TOPIC])
    parser.add_argument(
        "--report-only",
        action="store_true",
        help="draw the charts and print the table from the saved results",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=OUT_DIR,
        help="where the results are saved (default: docs/benchmarks)",
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

    report(out_dir)


if __name__ == "__main__":
    main()
