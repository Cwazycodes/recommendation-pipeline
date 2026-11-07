import json
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

import pytest

from realtime_recommendation_pipeline.tools.metrics_report import (
    Ingest,
    Latency,
    Source,
    format_report,
    prometheus_query,
    read_ingest,
    read_latency,
)

START, END = 1_790_000_000.0, 1_790_000_120.0  # a two-minute run
SOURCE = Source("viewing-events-demo", "flink-validate")


def fake_prometheus(answers: dict[str, float | None]) -> Any:
    """A Query that answers by the first key found in the expression."""
    asked: list[tuple[str, float]] = []

    def query(expression: str, at: float) -> float | None:
        asked.append((expression, at))
        for key, value in answers.items():
            if key in expression:
                return value
        return None

    query.asked = asked  # type: ignore[attr-defined]
    return query


def test_throughput_is_events_per_second_over_the_run() -> None:
    query = fake_prometheus(
        {
            'increase(kafka_topic_partition_current_offset{topic="viewing-events-demo"}': 6000.0,
            "increase(flink_taskmanager_job_task_operator_numRecordsOut": 5880.0,
            "deriv(": 0.2,
            "max_over_time(": 340.0,
            "min_over_time(": 12.0,
            "watermarkLag": 30.4,
        }
    )

    ingest = read_ingest(query, SOURCE, START, END)

    assert ingest == Ingest(
        produced_per_s=50.0,
        events_per_s=49.0,
        lag_end=12.0,
        lag_max=340.0,
        lag_slope_per_s=0.2,
        watermark_delay_s=30.4,
    )
    # Every number is for the run, evaluated at its end.
    assert {at for _expression, at in query.asked} == {END}


def test_lag_is_the_jobs_group_on_its_topic_just_after_checkpoints() -> None:
    query = fake_prometheus({})

    read_ingest(query, SOURCE, START, END)

    group_lag = (
        'sum(kafka_consumergroup_lag{consumergroup="flink-validate", '
        'topic="viewing-events-demo"})'
    )
    asked = [expression for expression, _at in query.asked]
    assert f"min_over_time({group_lag}[15s:5s])" in asked
    assert f"deriv((min_over_time({group_lag}[15s:5s]))[120s:5s])" in asked


@pytest.mark.parametrize(
    ("slope", "trend"),
    [(0.2, "steady"), (-0.4, "steady"), (0.6, "growing"), (-0.6, "shrinking")],
)
def test_lag_grows_when_it_rises_by_more_than_one_record_in_a_hundred(
    slope: float, trend: str
) -> None:
    ingest = Ingest(50.0, 50.0, 0.0, 0.0, slope, 30.0)

    assert ingest.lag_trend == trend


def test_processing_latency_is_freshness_above_the_watermark_delay() -> None:
    latency = Latency(
        windows=12.0,
        freshness={0.5: 37.0, 0.95: 41.5, 0.99: None},
        delivery={0.5: 4.0, 0.95: 9.0, 0.99: 9.8},
    )

    assert latency.processing == {0.5: 7.0, 0.95: 11.5, 0.99: None}


def test_latency_percentiles_come_from_the_writer_histograms() -> None:
    query = fake_prometheus(
        {
            "histogram_quantile(0.5, sum by (le) (increase(trending_window_freshness": 37.0,
            "histogram_quantile(0.95, sum by (le) (increase(trending_window_fresh": 41.0,
            "histogram_quantile(0.99, sum by (le) (increase(trending_window_fresh": 42.0,
            "trending_window_delivery_seconds_bucket": 4.0,
            "trending_windows_written_total": 11.6,
        }
    )

    latency = read_latency(query, START, END)

    assert latency.windows == 11.6
    assert latency.freshness == {0.5: 37.0, 0.95: 41.0, 0.99: 42.0}
    assert latency.delivery == {0.5: 4.0, 0.95: 4.0, 0.99: 4.0}


def test_report_shows_missing_numbers_as_not_available() -> None:
    ingest = Ingest(None, 49.6, 12.0, 340.0, None, None)
    latency = Latency(None, {0.5: 37.0, 0.95: None, 0.99: None}, {})

    lines = format_report(ingest, latency, START, END)

    assert lines[0].startswith("Metrics from 14:13:20 to 14:15:20 UTC")
    assert "49.6 events/s read by the job (n/a produced)" in lines[1]
    assert "trend n/a (unknown)" in lines[2]
    assert "p50  37.0 s  p95     n/a" in lines[5]
    assert "p50   7.0 s" in lines[6]


# Prometheus HTTP API answers, by query.
ANSWERS: dict[str, Any] = {
    "vector": {
        "status": "success",
        "data": {"resultType": "vector", "result": [{"value": [END, "49.5"]}]},
    },
    "empty": {"status": "success", "data": {"resultType": "vector", "result": []}},
    "nan": {
        "status": "success",
        "data": {"resultType": "vector", "result": [{"value": [END, "NaN"]}]},
    },
    "scalar": {
        "status": "success",
        "data": {"resultType": "scalar", "result": [END, "2"]},
    },
}


class Answers(BaseHTTPRequestHandler):
    """Answers each query with its canned body from ANSWERS."""

    def do_GET(self) -> None:
        params = parse_qs(urlparse(self.path).query)
        body = json.dumps(ANSWERS[params["query"][0]]).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args: Any) -> None:
        pass


@pytest.fixture
def prometheus_url() -> Iterator[str]:
    server = HTTPServer(("127.0.0.1", 0), Answers)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{server.server_port}"
    server.shutdown()


def test_prometheus_answers_become_numbers_or_none(prometheus_url: str) -> None:
    query = prometheus_query(prometheus_url)

    assert query("vector", END) == 49.5
    assert query("scalar", END) == 2.0
    assert query("empty", END) is None
    assert query("nan", END) is None
