"""Prometheus metrics for the results writer: how late each window is written.

docs/metrics.md defines each number. Prometheus scrapes them from the
writer's own HTTP endpoint (METRICS_WRITER_HOST and METRICS_WRITER_PORT).
"""

from __future__ import annotations

from datetime import datetime
from wsgiref.simple_server import WSGIServer

from prometheus_client import CollectorRegistry, Counter, Histogram, start_http_server

# Seconds. p50, p95 and p99 are estimated from these buckets, to within a
# bucket's width. In a live run a window is written about 30 s after it ends
# (the watermark delay) plus up to a checkpoint interval (10 s), so the
# buckets are 0.5 s wide from 30 to 45 s.
FRESHNESS_BUCKETS = (
    5, 10, 15, 20, 25, *(30 + step / 2 for step in range(31)),
    50, 55, 60, 75, 90, 120, 180, 300, 600,
)  # fmt: skip
# Seconds from Flink emitting a window to the writer storing it: the wait for
# the checkpoint that commits the Kafka transaction (up to 10 s), plus the
# writer's 2 s idle flush. 0.5 s buckets up to 12 s.
DELIVERY_BUCKETS = (
    0.25, *(step / 2 for step in range(1, 25)), 15, 20, 30, 60, 120,
)  # fmt: skip


class WriterMetrics:
    """The writer's metrics, in their own registry so each writer has its own."""

    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry or CollectorRegistry()
        self.freshness = Histogram(
            "trending_window_freshness_seconds",
            "written_at minus window_end for each window written",
            buckets=FRESHNESS_BUCKETS,
            registry=self.registry,
        )
        self.delivery = Histogram(
            "trending_window_delivery_seconds",
            "written_at minus generated_at (when Flink emitted the window)",
            buckets=DELIVERY_BUCKETS,
            registry=self.registry,
        )
        self.written = Counter(
            "trending_windows_written",
            "Windows written to the table",
            registry=self.registry,
        )
        self.dead_lettered = Counter(
            "trending_windows_dead_lettered",
            "Windows sent to the writer DLQ instead",
            registry=self.registry,
        )

    def window_written(
        self, window_end: datetime, generated_at: datetime, written_at: datetime
    ) -> None:
        self.freshness.observe((written_at - window_end).total_seconds())
        self.delivery.observe((written_at - generated_at).total_seconds())
        self.written.inc()

    def serve(self, host: str, port: int) -> WSGIServer:
        """Serve /metrics from a background thread.

        To stop, shut the server down and close it, which frees the port.
        """
        server, _thread = start_http_server(port, addr=host, registry=self.registry)
        return server
