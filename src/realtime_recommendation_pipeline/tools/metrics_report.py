"""Throughput, consumer lag and latency of a live run, read from Prometheus.

docs/metrics.md defines every number printed here. make metrics reports the
last few minutes (MINUTES=5); make demo reports its own run when it ends.
"""

from __future__ import annotations

import argparse
import json
import math
import re
import sys
import time
import urllib.parse
import urllib.request
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from realtime_recommendation_pipeline.flink_jobs.event_time import (
    MAX_OUT_OF_ORDERNESS_S,
)
from realtime_recommendation_pipeline.settings import (
    KafkaSettings,
    MetricsSettings,
    load,
)

# An instant query evaluated at a time (epoch seconds): its value, or None
# when Prometheus has no data for it.
Query = Callable[[str, float], float | None]
# The same for a query with one series per value of a label: label -> value.
QueryByLabel = Callable[[str, float], dict[str, float]]

# A job's Kafka source, by operator name: viewing-events-source in
# jobs/validate_events.py, continue-watching-source in jobs/continue_watching.py.
# Both jobs read the events topic, so every query picks one of them.
OPERATOR = "flink_taskmanager_job_task_operator"
TRENDING_SOURCE = "viewing-events-source"
CONTINUE_WATCHING_SOURCE = "continue-watching-source"
RECORDS_READ = f"{OPERATOR}_numRecordsOut"
WATERMARK_LAG_MS = f"{OPERATOR}_watermarkLag"
# From kafka-exporter: each partition's end offset.
END_OFFSETS = "kafka_topic_partition_current_offset"
# From the job's Kafka source: the offset of the last record it read in each
# partition, -1 before the first.
READ_OFFSETS = f"{OPERATOR}_KafkaSourceReader_topic_partition_currentOffset"
FRESHNESS = "trending_window_freshness_seconds"
DELIVERY = "trending_window_delivery_seconds"
WRITE = "trending_window_write_seconds"
WINDOWS_WRITTEN = "trending_windows_written_total"

QUANTILES = (0.5, 0.95, 0.99)
# Lag counts as growing when it rises by more than 1 record for every 100 read.
GROWING = 0.01


class MetricsError(Exception):
    """Prometheus could not answer a query."""


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
    # How long the DynamoDB put of each window took.
    write: dict[float, float | None] = field(default_factory=dict)

    @property
    def processing(self) -> dict[float, float | None]:
        """Freshness above the watermark delay."""
        return {
            q: None if value is None else value - MAX_OUT_OF_ORDERNESS_S
            for q, value in self.freshness.items()
        }


def instant_query(
    prometheus_url: str, expression: str, at: float, timeout_s: float
) -> dict[str, Any]:
    """The data of a Prometheus HTTP API instant query evaluated at a time."""
    params = urllib.parse.urlencode({"query": expression, "time": f"{at:.3f}"})
    url = f"{prometheus_url}/api/v1/query?{params}"
    with urllib.request.urlopen(url, timeout=timeout_s) as response:
        body = json.load(response)
    if body.get("status") != "success":
        raise MetricsError(f"{expression}: {body.get('error')}")

    data: dict[str, Any] = body["data"]
    return data


def finite(text: str) -> float | None:
    """A sample's value, or None for NaN and infinity."""
    value = float(text)
    return None if math.isnan(value) or math.isinf(value) else value


def prometheus_query(prometheus_url: str, timeout_s: float = 10) -> Query:
    """A Query that asks the Prometheus HTTP API."""

    def query(expression: str, at: float) -> float | None:
        data = instant_query(prometheus_url, expression, at, timeout_s)
        sample = data["result"] if data["resultType"] == "scalar" else None
        if data["resultType"] == "vector" and data["result"]:
            sample = data["result"][0]["value"]

        return None if sample is None else finite(sample[1])

    return query


def prometheus_query_by_label(
    prometheus_url: str, label: str, timeout_s: float = 10
) -> QueryByLabel:
    """A QueryByLabel that asks the Prometheus HTTP API; NaN values are left out."""

    def query(expression: str, at: float) -> dict[str, float]:
        data = instant_query(prometheus_url, expression, at, timeout_s)
        values = {
            series["metric"].get(label, ""): finite(series["value"][1])
            for series in data["result"]
        }

        return {key: value for key, value in values.items() if value is not None}

    return query


def lookback(seconds: float) -> str:
    """A Prometheus range for a stretch this long, at least two 5 s scrapes."""
    return f"{max(round(seconds), 10)}s"


def per_second(total: float | None, seconds: float) -> float | None:
    return None if total is None else total / max(seconds, 1)


def flink_label(value: str) -> str:
    """A label value as Flink's reporter writes it: viewing_events_demo."""
    return re.sub(r"[^a-zA-Z0-9:_]", "_", value)


def source_label(source: str) -> str:
    """The label that picks one job's Kafka source; Flink's reporter writes
    Source: before the name and turns its hyphens into underscores."""
    return f'operator_name="Source:_{flink_label(source)}"'


def read_lag(topic: str, source: str = TRENDING_SOURCE) -> str:
    """Records on the topic the job has not read yet, summed over partitions:
    each end offset minus the next offset to read (currentOffset + 1)."""
    read = f'{READ_OFFSETS}{{topic="{flink_label(topic)}",{source_label(source)}}}'
    return f'sum({END_OFFSETS}{{topic="{topic}"}}) - sum({read}) - count({read})'


def read_ingest(
    query: Query, topic: str, start: float, end: float, source: str = TRENDING_SOURCE
) -> Ingest:
    span = lookback(end - start)
    lag = read_lag(topic, source)
    selector = source_label(source)
    # Kafka and Flink are scraped a moment apart, so a small lag can read
    # below 0; the trend uses the raw values.
    shown_lag = f"clamp_min({lag}, 0)"

    return Ingest(
        produced_per_s=per_second(
            query(f'sum(increase({END_OFFSETS}{{topic="{topic}"}}[{span}]))', end),
            end - start,
        ),
        events_per_s=per_second(
            query(f"sum(increase({RECORDS_READ}{{{selector}}}[{span}]))", end),
            end - start,
        ),
        lag_end=query(shown_lag, end),
        lag_max=query(f"max_over_time(({shown_lag})[{span}:5s])", end),
        lag_slope_per_s=query(f"deriv(({lag})[{span}:5s])", end),
        watermark_delay_s=query(f"max({WATERMARK_LAG_MS}{{{selector}}}) / 1000", end),
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
        write={q: quantile(query, WRITE, q, span, end) for q in QUANTILES},
    )


def number(value: float | None, unit: str = "", digits: int = 1) -> str:
    return "n/a" if value is None else f"{value:.{digits}f}{unit}"


def percentiles(
    values: dict[float, float | None],
    unit: str = " s",
    scale: float = 1,
    digits: int = 1,
) -> str:
    return "  ".join(
        f"p{round(q * 100)} "
        f"{number(None if value is None else value * scale, unit, digits):>7}"
        for q, value in values.items()
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
        f"  DynamoDB put     {percentiles(latency.write, ' ms', 1000, 0)}",
    ]


def report(
    query: Query,
    topic: str,
    ingest_from: float,
    ingest_to: float,
    latency_from: float,
    latency_to: float,
) -> list[str]:
    """The report lines; ingest and latency can cover different stretches."""
    return format_report(
        read_ingest(query, topic, ingest_from, ingest_to),
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
    topic = args.topic or load(KafkaSettings).events_topic
    end = time.time()
    start = end - args.minutes * 60

    try:
        query = prometheus_query(prometheus_url)
        lines = report(query, topic, start, end, start, end)
    except (OSError, MetricsError) as exc:
        print(f"Could not read metrics from {prometheus_url}: {exc}", file=sys.stderr)
        sys.exit(1)

    print("\n".join(lines))


if __name__ == "__main__":
    main()
