from realtime_recommendation_pipeline.tools.bench import Run, TaskTime
from realtime_recommendation_pipeline.tools.bench_continue_watching import (
    between,
    markdown_table,
    percentile,
    result_row,
)
from realtime_recommendation_pipeline.tools.metrics_report import Ingest

RUN = Run(parallelism=1, partitions=6, rate=100.0, duration_s=120.0, seed=42)
INGEST = Ingest(
    produced_per_s=100.0,
    events_per_s=99.6,
    lag_end=12.0,
    lag_max=140.0,
    lag_slope_per_s=0.1,
    watermark_delay_s=None,
)


def test_percentile_is_the_nearest_rank() -> None:
    values = [float(n) for n in range(1, 101)]

    assert percentile(values, 0.5) == 50.0
    assert percentile(values, 0.95) == 95.0
    assert percentile(values, 0.99) == 99.0
    assert percentile([7.0], 0.99) == 7.0
    assert percentile([], 0.5) is None


def test_only_samples_inside_the_run_are_kept() -> None:
    samples = [(10.0, 1.0), (20.0, 2.0), (30.0, 3.0)]

    assert between(samples, 15.0, 30.0) == [2.0, 3.0]


def test_a_run_becomes_one_row_and_one_table_line() -> None:
    row = result_row(
        RUN,
        started_at=1_793_000_000.0,
        ingest=INGEST,
        latency_s=[4.0, 5.0, 6.0, 12.5],
        write_s=[0.02, 0.05],
        tasks={
            "continue-watching-source": TaskTime(310.0, 0.0),
            "track-progress": TaskTime(640.5, 0.0),
        },
    )

    assert row["lag_trend"] == "steady"
    assert row["items_written"] == "4"
    assert (row["latency_p50_s"], row["latency_p95_s"]) == ("5", "12.5")
    assert row["batch_write_p99_ms"] == "50"
    assert row["busiest_task"] == "track-progress"
    assert markdown_table([row])[-1] == (
        "| 1 | 100 | 100 | 12 | steady | 4 | 5.0 / 12.5 / 12.5 | 20 / 50 / 50 "
        "| track-progress (640) |"
    )
