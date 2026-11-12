"""One-command demo of the whole pipeline on the local stack.

Live viewing events go into Kafka, the Flink job ranks them in 2-minute windows
sliding every 10 seconds, the results writer copies each window into DynamoDB
Local, and the API's Lambda handler serves the latest list. At the end it
prints the run's throughput, lag and latency from Prometheus. The demo has its
own topics and partition key, so it never touches the 1-hour pipeline's data.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import threading
import time
import urllib.request
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from botocore.exceptions import BotoCoreError, ClientError
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from realtime_recommendation_pipeline.common.trending_table import (
    TIME_FORMAT,
    create_table_if_missing,
    dynamodb_resource,
    latest_window,
    load_table_definition,
)
from realtime_recommendation_pipeline.producer.kafka_producer import produce_events
from realtime_recommendation_pipeline.settings import (
    DynamoDBSettings,
    FlinkSettings,
    KafkaSettings,
    MetricsSettings,
    load,
)
from realtime_recommendation_pipeline.tools.api_local import (
    call_handler,
    print_response,
)
from realtime_recommendation_pipeline.tools.metrics_report import (
    MetricsError,
    prometheus_query,
    report,
)
from realtime_recommendation_pipeline.writer.metrics import WriterMetrics
from realtime_recommendation_pipeline.writer.results_writer import (
    NO_SDK_RETRIES,
    KafkaDeadLetters,
    ResultsWriter,
    writer_consumer_config,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
TABLE_DEFINITION = REPO_ROOT / "infra" / "dynamodb" / "trending-table.json"

WINDOW_SIZE_S = 120
WINDOW_SLIDE_S = 10
PARTITION_KEY = "TRENDING#2m"

# Topic name -> partitions. They are deleted and created again on every run.
EVENTS_TOPIC = "viewing-events-demo"
TRENDING_TOPIC = "trending-results-demo"
EVENTS_DLQ_TOPIC = "viewing-events-dlq-demo"
WRITER_DLQ_TOPIC = "trending-writer-dlq-demo"
TOPICS = {EVENTS_TOPIC: 6, TRENDING_TOPIC: 1, EVENTS_DLQ_TOPIC: 6, WRITER_DLQ_TOPIC: 1}

# The agreed time: the first window must reach DynamoDB this soon after the
# first event. By construction it takes about 50 s: the first window ends
# within 10 s, fires 30 s later when the watermark passes it, and is committed
# by the next checkpoint (every 10 s). The rest is room for a slow laptop.
RESULTS_WITHIN_S = 90

DEMO_JOB_ARGS = [
    "--window-size-seconds", str(WINDOW_SIZE_S),
    "--window-slide-seconds", str(WINDOW_SLIDE_S),
    "--source-topic", EVENTS_TOPIC,
    "--trending-topic", TRENDING_TOPIC,
    "--dlq-topic", EVENTS_DLQ_TOPIC,
]  # fmt: skip


class DemoError(Exception):
    """A step of the demo failed; the message says which and what to do."""


@dataclass(frozen=True)
class DemoSettings:
    """Where the stack is (from the environment), and how long to run."""

    kafka: KafkaSettings
    flink: FlinkSettings
    dynamo: DynamoDBSettings
    metrics: MetricsSettings
    duration_s: float = 120.0
    rate: float = 50.0
    limit: int = 10

    @classmethod
    def from_env(cls, **options: Any) -> DemoSettings:
        return cls(
            load(KafkaSettings),
            load(FlinkSettings),
            load(DynamoDBSettings),
            load(MetricsSettings),
            **options,
        )

    @property
    def bootstrap_servers(self) -> str:
        return self.kafka.bootstrap_servers

    @property
    def flink_url(self) -> str:
        return self.flink.rest_url

    @property
    def dynamodb_endpoint(self) -> str | None:
        return self.dynamo.endpoint_url


@dataclass(frozen=True)
class DemoResult:
    first_window_after_s: float | None
    windows_seen: int
    events_delivered: int
    response: dict[str, Any]


def flink_request(
    url: str, body: dict[str, Any] | None = None, timeout_s: float = 10
) -> Any:
    """GET (or POST a JSON body to) the Flink REST API."""
    request = urllib.request.Request(
        url,
        data=None if body is None else json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
        method="GET" if body is None else "POST",
    )
    with urllib.request.urlopen(request, timeout=timeout_s) as response:
        return json.load(response)


def wait_until(
    description: str, ready: Callable[[], bool], timeout_s: float = 300
) -> None:
    """Poll ready() every 2 s; raise DemoError if it is not true in time.

    Connection errors count as "not ready yet"; any other error is raised.
    """
    deadline = time.monotonic() + timeout_s
    last_error: Exception | None = None
    while True:
        try:
            if ready():
                return
        except (OSError, KafkaException, BotoCoreError, ClientError) as exc:
            last_error = exc

        if time.monotonic() > deadline:
            detail = f" Last error: {last_error}" if last_error else ""
            raise DemoError(f"{description} after {timeout_s:.0f} s.{detail}")
        time.sleep(2)


def wait_for_services(settings: DemoSettings) -> None:
    admin = AdminClient({"bootstrap.servers": settings.bootstrap_servers})
    wait_until(
        f"Kafka is not reachable at {settings.bootstrap_servers} (is `make up` done?)",
        lambda: admin.list_topics(timeout=5) is not None,
    )
    wait_until(
        f"Flink at {settings.flink_url} has no free task slot",
        lambda: flink_request(f"{settings.flink_url}/overview")["slots-available"] > 0,
    )
    # DynamoDB in AWS is not checked: the writer's role may only put items.
    if settings.dynamodb_endpoint is None:
        return
    client = dynamodb_resource(settings.dynamodb_endpoint).meta.client
    wait_until(
        f"DynamoDB Local is not reachable at {settings.dynamodb_endpoint}",
        lambda: "TableNames" in client.list_tables(),
    )


def recreate_topics(bootstrap_servers: str, topics: Mapping[str, int] = TOPICS) -> None:
    """Delete these topics if they exist and create them empty."""
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})

    existing = admin.list_topics(timeout=10).topics
    stale = [topic for topic in topics if topic in existing]
    for future in admin.delete_topics(stale).values() if stale else []:
        future.result(30)

    # Deletion finishes in the background, so creating can briefly fail with
    # "already exists"; try again until it works.
    pending = dict(topics)
    deadline = time.monotonic() + 60
    while pending:
        futures = admin.create_topics(
            [NewTopic(topic, partitions, 1) for topic, partitions in pending.items()]
        )
        for topic, future in futures.items():
            try:
                future.result(30)
                del pending[topic]
            except KafkaException as exc:
                if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                    raise
        if pending and time.monotonic() > deadline:
            raise DemoError(f"Could not create demo topics {sorted(pending)}")
        if pending:
            time.sleep(1)


def submit_job(
    job_args: Sequence[str] = DEMO_JOB_ARGS, script: str = "validate_events.py"
) -> str:
    """Submit a streaming job from jobs/ with these arguments; return its id."""
    command = [
        "docker", "compose", "exec", "-T", "flink-jobmanager",
        "flink", "run", "-d",
        "-py", f"/opt/flink/jobs/{script}",
        "-pyfs", "/opt/flink/src",
        *job_args,
    ]  # fmt: skip
    result = subprocess.run(
        command, check=False, cwd=REPO_ROOT, capture_output=True, text=True
    )

    match = re.search(r"JobID ([0-9a-f]{32})", result.stdout)
    if result.returncode or match is None:
        raise DemoError(
            f"Could not submit the Flink job:\n{result.stdout}{result.stderr}"
        )

    return match.group(1)


def job_is_running(flink_url: str, job_id: str) -> bool:
    job = flink_request(f"{flink_url}/jobs/{job_id}")
    if job["state"] in ("FAILED", "CANCELED", "FINISHED"):
        raise DemoError(
            f"The Flink job is {job['state']}; see {flink_url}/#/job/{job_id}"
        )

    return job["state"] == "RUNNING" and all(
        vertex["status"] == "RUNNING" for vertex in job["vertices"]
    )


def stop_job(flink: FlinkSettings, job_id: str) -> None:
    """Stop the job with a savepoint, which is safe with RocksDB unlike cancel."""
    flink_request(
        f"{flink.rest_url}/jobs/{job_id}/stop",
        {"targetDirectory": f"{flink.checkpoint_dir}/savepoints", "drain": False},
        timeout_s=30,
    )
    wait_until(
        "The demo job did not stop",
        lambda: flink_request(f"{flink.rest_url}/jobs/{job_id}")["state"] == "FINISHED",
        timeout_s=60,
    )


class WriterThread(threading.Thread):
    """The results writer on the demo topic, in the background."""

    def __init__(self, settings: DemoSettings) -> None:
        super().__init__(name="results-writer", daemon=True)
        self.settings = settings
        self.stopping = threading.Event()

    def run(self) -> None:
        # A new group each run, so no offsets are left over from earlier runs.
        group_id = f"results-writer-demo-{uuid.uuid4()}"
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
        # Served where make writer serves them, so Prometheus scrapes this run.
        metrics = WriterMetrics()
        server = metrics.serve(
            self.settings.metrics.writer_host, self.settings.metrics.writer_port
        )

        writer = ResultsWriter(
            consumer,
            dynamodb.Table(self.settings.dynamo.table),
            KafkaDeadLetters(producer, WRITER_DLQ_TOPIC),
            partition_key=PARTITION_KEY,
            metrics=metrics,
        )
        consumer.subscribe(
            [TRENDING_TOPIC], on_revoke=lambda _c, _p: writer.buffer.clear()
        )

        try:
            writer.run(self.stopping.is_set)
        finally:
            consumer.close()
            server.shutdown()
            server.server_close()


class ProducerThread(threading.Thread):
    """Live viewing events at a steady rate, in the background."""

    def __init__(self, settings: DemoSettings) -> None:
        super().__init__(name="producer", daemon=True)
        self.settings = settings
        self.stats: dict[str, int] | None = None

    def run(self) -> None:
        self.stats = produce_events(
            bootstrap_servers=self.settings.bootstrap_servers,
            rate=self.settings.rate,
            duration=self.settings.duration_s,
            topic=EVENTS_TOPIC,
            live=True,
        )


def print_metrics(
    settings: DemoSettings,
    say: Callable[[str], None],
    produced_from: float,
    produced_to: float,
) -> None:
    """The run's metrics: throughput and lag while producing, latency since."""
    url = settings.metrics.prometheus_url
    try:
        lines = report(
            prometheus_query(url),
            EVENTS_TOPIC,
            produced_from,
            produced_to,
            produced_from,
            time.time(),
        )
    except (OSError, MetricsError) as exc:
        say(f"Could not read metrics from Prometheus at {url}: {exc}")
        return

    for line in lines:
        say(line)


def run_demo(settings: DemoSettings, say: Callable[[str], None] = print) -> DemoResult:
    """Run the pipeline end to end and return what reached the API."""
    if settings.dynamodb_endpoint is None:
        raise DemoError(
            "make demo runs on DynamoDB Local (DYNAMODB_ENDPOINT_URL). For DynamoDB "
            "in AWS, use make writer-aws or make bench-aws (docs/design.md)"
        )
    started = time.monotonic()

    def log(message: str) -> None:
        say(f"[{time.monotonic() - started:4.0f}s] {message}")

    # Real requests rather than Docker health checks, which start a JVM each
    # time and can time out on a busy laptop while the services work fine.
    log(
        "Waiting for Kafka, Flink and DynamoDB Local (up to 5 minutes on a first start)"
    )
    wait_for_services(settings)

    # What make topics and make db-init do, for the demo's topics and table.
    dynamodb = dynamodb_resource(settings.dynamodb_endpoint)
    create_table_if_missing(
        dynamodb.meta.client, load_table_definition(TABLE_DEFINITION)
    )
    table = dynamodb.Table(settings.dynamo.table)

    recreate_topics(settings.bootstrap_servers)
    log(f"Created empty demo topics: {', '.join(TOPICS)}")

    job_id = submit_job()
    writer = WriterThread(settings)
    try:
        log(
            f"Submitted the Flink job ({WINDOW_SIZE_S} s windows every {WINDOW_SLIDE_S} s)"
        )
        wait_until(
            "The Flink job did not start running",
            lambda: job_is_running(settings.flink_url, job_id),
            timeout_s=120,
        )
        writer.start()

        # Windows from earlier demo runs end before this moment.
        run_started = datetime.now(UTC).strftime(TIME_FORMAT)
        producer = ProducerThread(settings)
        producer.start()
        first_event_at = time.monotonic()
        produced_from = time.time()
        log(
            f"Job running, writer started. Producing {settings.rate:.0f} live events/s "
            f"for {settings.duration_s:.0f} s; results expected within "
            f"{RESULTS_WITHIN_S} s"
        )

        first_window_after_s: float | None = None
        windows_seen = 0
        last_window_end = ""
        last_change = time.monotonic()
        producer_done_at: float | None = None
        produced_to = time.time()

        while True:
            try:
                latest = latest_window(table, datetime.now(UTC), PARTITION_KEY)
            except (BotoCoreError, ClientError) as exc:
                log(f"DynamoDB Local did not answer ({exc}); trying again")
                latest = None

            if latest and latest["sk"] > max(last_window_end, run_started):
                last_window_end = latest["sk"]
                windows_seen += 1
                last_change = time.monotonic()
                if first_window_after_s is None:
                    first_window_after_s = last_change - first_event_at
                    log(
                        f"First window reached DynamoDB {first_window_after_s:.0f} s "
                        "after the first event"
                    )
                top = latest["items"][0]
                log(
                    f"Window ending {last_window_end[11:19]} UTC: #1 is "
                    f"{top['programme_id']} with {top['score']} viewers"
                )

            if not writer.is_alive():
                raise DemoError("The results writer stopped; its error is above")

            if producer_done_at is None and not producer.is_alive():
                if producer.stats is None:
                    raise DemoError("The producer stopped; its error is above")
                producer_done_at = time.monotonic()
                produced_to = time.time()
                log(
                    f"Produced {producer.stats['delivered']} events; waiting for "
                    "the last windows"
                )

            # Done once no new window has arrived for 15 s after producing stops.
            if producer_done_at is not None:
                quiet_for = time.monotonic() - max(last_change, producer_done_at)
                if quiet_for >= 15 or time.monotonic() - producer_done_at >= 60:
                    break

            time.sleep(1)

        log(f"GET /trending?limit={settings.limit} on {PARTITION_KEY}:")
        demo_list = settings.dynamo.model_copy(update={"partition_key": PARTITION_KEY})
        response = call_handler(demo_list, str(settings.limit))
        print_response(response)
        print_metrics(settings, say, produced_from, produced_to)

        return DemoResult(
            first_window_after_s=first_window_after_s,
            windows_seen=windows_seen,
            events_delivered=producer.stats["delivered"] if producer.stats else 0,
            response=response,
        )
    finally:
        writer.stopping.set()
        if writer.is_alive():
            writer.join(timeout=15)
        try:
            stop_job(settings.flink, job_id)
            log("Stopped the writer and the demo job (savepoint kept)")
        except (OSError, DemoError) as exc:
            log(
                f"Could not stop the demo job {job_id}: {exc}. Stop it in the Flink UI."
            )


def main(argv: Sequence[str] | None = None) -> None:
    """Kafka, Flink and DynamoDB addresses come from the environment (settings.py)."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duration", type=float, default=120.0)
    parser.add_argument("--rate", type=float, default=50.0)
    args = parser.parse_args(argv)

    settings = DemoSettings.from_env(duration_s=args.duration, rate=args.rate)

    try:
        result = run_demo(settings)
    except DemoError as exc:
        print(f"Demo failed: {exc}", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        sys.exit(130)

    sys.exit(0 if result.response["statusCode"] == 200 else 1)


if __name__ == "__main__":
    main()
