"""Throughput, consumer lag and latency of a live run, read from Prometheus.

docs/metrics.md defines every number printed here. make metrics reports the
last few minutes (MINUTES=5); make demo reports its own run when it ends.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
import time
import urllib.parse
import urllib.request
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime

from realtime_recommendation_pipeline.flink_jobs.event_time import (
    MAX_OUT_OF_ORDERNESS_S,
)
from realtime_recommendation_pipeline.settings import (
    FlinkSettings,
    KafkaSettings,
    MetricsSettings,
    load,
)

# An instant query evaluated at a time (epoch seconds): its value, or None
# when Prometheus has no data for it.
Query = Callable[[str, float], float | None]

# The job's Kafka source, viewing-events-source in jobs/validate_events.py;
# Flink's reporter turns the hyphens into underscores.
OPERATOR = "flink_taskmanager_job_task_operator"
SOURCE = 'operator_name="Source:_viewing_events_source"'
RECORDS_READ = f"{OPERATOR}_numRecordsOut{{{SOURCE}}}"
WATERMARK_LAG_MS = f"{OPERATOR}_watermarkLag{{{SOURCE}}}"
# From kafka-exporter: each partition's end offset, and each group's lag.
END_OFFSETS = "kafka_topic_partition_current_offset"
GROUP_LAG = "kafka_consumergroup_lag"
# The job commits its offsets when a checkpoint completes (every 10 s), so
# its group's lag climbs between checkpoints. The lowest value in any 15 s is
# the lag just after a checkpoint.
SETTLED = "15s"
FRESHNESS = "trending_window_freshness_seconds"
DELIVERY = "trending_window_delivery_seconds"
WINDOWS_WRITTEN = "trending_windows_written_total"

QUANTILES = (0.5, 0.95, 0.99)
# Lag counts as growing when it rises by more than 1 record for every 100 read.
GROWING = 0.01


class MetricsError(Exception):
    """Prometheus could not answer a query."""


@dataclass(frozen=True)
class Source:
    """The topic the job reads, and the consumer group it commits offsets as."""

    topic: str
    group: str


@dataclass(frozen=True)
class Ingest:
    """How fast events arrived and the job read them, and whether it kept up."""

    produced_per_s: float | None
    events_per_s: float | None
    lag_end: float | None
    lag_max: float | None
    lag_slope_per_s: float | None
    watermark_delay_s: float | None

    @property
    def lag_trend(self) -> str:
        if self.lag_slope_per_s is None or not self.events_per_s:
            return "unknown"
        if self.lag_slope_per_s > GROWING * self.events_per_s:
            return "growing"
        if self.lag_slope_per_s < -GROWING * self.events_per_s:
            return "shrinking"
        return "steady"


@dataclass(frozen=True)
class Latency:
    """Percentiles of the windows written, in seconds, by quantile."""

    windows: float | None
    freshness: dict[float, float | None]
    delivery: dict[float, float | None]

    @property
    def processing(self) -> dict[float, float | None]:
        """Freshness above the watermark delay."""
        return {
            q: None if value is None else value - MAX_OUT_OF_ORDERNESS_S
            for q, value in self.freshness.items()
        }


def prometheus_query(prometheus_url: str, timeout_s: float = 10) -> Query:
    """A Query that asks the Prometheus HTTP API."""

    def query(expression: str, at: float) -> float | None:
        params = urllib.parse.urlencode({"query": expression, "time": f"{at:.3f}"})
        url = f"{prometheus_url}/api/v1/query?{params}"
        with urllib.request.urlopen(url, timeout=timeout_s) as response:
            body = json.load(response)
        if body.get("status") != "success":
            raise MetricsError(f"{expression}: {body.get('error')}")

        data = body["data"]
        sample = data["result"] if data["resultType"] == "scalar" else None
        if data["resultType"] == "vector" and data["result"]:
            sample = data["result"][0]["value"]
        if sample is None:
            return None

        value = float(sample[1])
        return None if math.isnan(value) or math.isinf(value) else value

    return query


def lookback(seconds: float) -> str:
    """A Prometheus range for a stretch this long, at least two 5 s scrapes."""
    return f"{max(round(seconds), 10)}s"


def per_second(total: float | None, seconds: float) -> float | None:
    return None if total is None else total / max(seconds, 1)


def read_ingest(query: Query, source: Source, start: float, end: float) -> Ingest:
    span = lookback(end - start)
    topic = f'topic="{source.topic}"'
    lag = f'sum({GROUP_LAG}{{consumergroup="{source.group}", {topic}}})'
    settled = f"min_over_time({lag}[{SETTLED}:5s])"

    return Ingest(
        produced_per_s=per_second(
            query(f"sum(increase({END_OFFSETS}{{{topic}}}[{span}]))", end), end - start
        ),
        events_per_s=per_second(
            query(f"sum(increase({RECORDS_READ}[{span}]))", end), end - start
        ),
        lag_end=query(settled, end),
        lag_max=query(f"max_over_time({lag}[{span}:5s])", end),
        lag_slope_per_s=query(f"deriv(({settled})[{span}:5s])", end),
        watermark_delay_s=query(f"max({WATERMARK_LAG_MS}) / 1000", end),
    )


def quantile(
    query: Query, histogram: str, q: float, span: str, at: float
) -> float | None:
    buckets = f"sum by (le) (increase({histogram}_bucket[{span}]))"
    return query(f"histogram_quantile({q}, {buckets})", at)


def read_latency(query: Query, start: float, end: float) -> Latency:
    span = lookback(end - start)

    return Latency(
        windows=query(f"sum(increase({WINDOWS_WRITTEN}[{span}]))", end),
        freshness={q: quantile(query, FRESHNESS, q, span, end) for q in QUANTILES},
        delivery={q: quantile(query, DELIVERY, q, span, end) for q in QUANTILES},
    )


def number(value: float | None, unit: str = "", digits: int = 1) -> str:
    return "n/a" if value is None else f"{value:.{digits}f}{unit}"


def percentiles(values: dict[float, float | None]) -> str:
    return "  ".join(
        f"p{round(q * 100)} {number(value, ' s'):>7}" for q, value in values.items()
    )


def format_report(
    ingest: Ingest, latency: Latency, start: float, end: float
) -> list[str]:
    def clock(at: float) -> str:
        return datetime.fromtimestamp(at, UTC).strftime("%H:%M:%S")

    bound = MAX_OUT_OF_ORDERNESS_S
    throughput = (
        f"{number(ingest.events_per_s)} events/s read by the job "
        f"({number(ingest.produced_per_s)} produced)"
    )
    lag = (
        f"{number(ingest.lag_end, digits=0)} records at the end, "
        f"at most {number(ingest.lag_max, digits=0)}, "
        f"trend {number(ingest.lag_slope_per_s, ' records/s')} ({ingest.lag_trend})"
    )
    watermark = f"{number(ingest.watermark_delay_s, ' s')} at the end (bound {bound} s)"

    return [
        f"Metrics from {clock(start)} to {clock(end)} UTC (see docs/metrics.md)",
        f"  Throughput       {throughput}",
        f"  Consumer lag     {lag}",
        f"  Watermark delay  {watermark}",
        f"  Windows written  {number(latency.windows, digits=0)}",
        f"  Freshness        {percentiles(latency.freshness)}",
        f"  Processing       {percentiles(latency.processing)}  (freshness - {bound} s)",
        f"  Delivery         {percentiles(latency.delivery)}",
    ]


def report(
    query: Query,
    source: Source,
    ingest_from: float,
    ingest_to: float,
    latency_from: float,
    latency_to: float,
) -> list[str]:
    """The report lines; ingest and latency can cover different stretches."""
    return format_report(
        read_ingest(query, source, ingest_from, ingest_to),
        read_latency(query, latency_from, latency_to),
        min(ingest_from, latency_from),
        max(ingest_to, latency_to),
    )


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--minutes", type=float, default=5.0)
    parser.add_argument(
        "--topic", help="the topic the job reads (default: KAFKA_EVENTS_TOPIC)"
    )
    args = parser.parse_args(argv)

    prometheus_url = load(MetricsSettings).prometheus_url
    source = Source(
        args.topic or load(KafkaSettings).events_topic,
        load(FlinkSettings).source_group_id,
    )
    end = time.time()
    start = end - args.minutes * 60

    try:
        query = prometheus_query(prometheus_url)
        lines = report(query, source, start, end, start, end)
    except (OSError, MetricsError) as exc:
        print(f"Could not read metrics from {prometheus_url}: {exc}", file=sys.stderr)
        sys.exit(1)

    print("\n".join(lines))


if __name__ == "__main__":
    main()
