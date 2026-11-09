from pathlib import Path

import pytest

from realtime_recommendation_pipeline.tools.bench import (
    RESULT_COLUMNS,
    Row,
    Run,
    Summary,
    TaskTime,
    cell,
    draw_charts,
    markdown_table,
    merge,
    read_rows,
    read_task_times,
    report,
    result_row,
    save_run,
    summarise,
    task_label,
    task_rows,
)
from realtime_recommendation_pipeline.tools.metrics_report import Ingest, Latency

# Task names as Flink's Prometheus reporter writes them for the job.
SOURCE_TASK = (
    "Source:_viewing_events_source____validate_events_____Map____"
    "viewing_events_dlq_sink:_Writer____viewing_events_dlq_sink:_Committer"
)
DEDUP_TASK = (
    "deduplicate_events__to_event_rows_____anonymous_datastream_source_1__1_____"
    "Calc_2_____LocalWindowAggregate_3_"
)
RANK_TASK = "WindowRank_8_____Calc_9_____ConstraintEnforcer_10_"

RUN = Run(parallelism=1, partitions=6, rate=200.0, duration_s=120.0, seed=42)
STARTED_AT = 1_790_000_000.0  # 2026-09-21T14:13:20Z


@pytest.mark.parametrize(
    ("name", "label"),
    [
        (SOURCE_TASK, "viewing-events-source"),
        (DEDUP_TASK, "deduplicate-events, to-event-rows"),
        ("GlobalWindowAggregate_5_____Calc_6_", "GlobalWindowAggregate"),
        (RANK_TASK, "WindowRank"),
    ],
)
def test_tasks_are_named_by_their_first_operator(name: str, label: str) -> None:
    assert task_label(name) == label


def test_task_times_are_the_busiest_subtasks_over_the_run() -> None:
    asked: list[tuple[str, float]] = []

    def query(expression: str, at: float) -> dict[str, float]:
        asked.append((expression, at))
        if "busyTimeMsPerSecond" in expression:
            return {DEDUP_TASK: 998.0, SOURCE_TASK: 310.5}
        return {SOURCE_TASK: 640.0}

    tasks = read_task_times(query, "abc123", STARTED_AT, STARTED_AT + 120)

    assert tasks == {
        "deduplicate-events, to-event-rows": TaskTime(998.0, None),
        "viewing-events-source": TaskTime(310.5, 640.0),
    }
    busy = (
        "max by (task_name) (avg_over_time("
        'flink_taskmanager_job_task_busyTimeMsPerSecond{job_id="abc123"}[120s]))'
    )
    assert asked[0] == (busy, STARTED_AT + 120)


@pytest.mark.parametrize(
    ("value", "text"),
    [(None, ""), (500.0, "500"), (36.25, "36.25"), (151.234, "151.23"), (-0.001, "0")],
)
def test_cells_have_at_most_two_decimals(value: float | None, text: str) -> None:
    assert cell(value) == text


def test_a_run_is_one_row_of_results_and_a_row_per_task() -> None:
    ingest = Ingest(
        produced_per_s=199.6,
        events_per_s=151.234,
        lag_end=5210.0,
        lag_max=5400.0,
        lag_slope_per_s=46.5,
        watermark_delay_s=61.0,
    )
    latency = Latency(
        windows=6.4,
        freshness={0.5: 40.25, 0.95: 44.0, 0.99: None},
        delivery={0.5: 2.0, 0.95: 2.5, 0.99: 2.5},
        write={0.5: 0.0123, 0.95: 0.0385, 0.99: None},
    )
    tasks = {
        "deduplicate-events, to-event-rows": TaskTime(998.0, None),
        "viewing-events-source": TaskTime(310.5, 640.0),
    }

    row = result_row(RUN, STARTED_AT, ingest, latency, tasks)

    assert list(row) == RESULT_COLUMNS
    assert row["started_at"] == "2026-09-21T14:13:20Z"
    assert (row["rate"], row["read_per_s"], row["lag_trend"]) == (
        "200",
        "151.23",
        "growing",
    )
    assert row["windows"] == "6"
    assert row["processing_p50_s"] == "10.25"
    assert row["processing_p99_s"] == ""
    assert (row["write_p50_ms"], row["write_p95_ms"], row["write_p99_ms"]) == (
        "12.3",
        "38.5",
        "",
    )
    assert row["busiest_task"] == "deduplicate-events, to-event-rows"
    assert row["busiest_task_busy_ms_per_s"] == "998"
    assert task_rows(RUN, tasks)[1] == {
        "parallelism": "1",
        "partitions": "6",
        "rate": "200",
        "task": "viewing-events-source",
        "busy_ms_per_s": "310.5",
        "backpressured_ms_per_s": "640",
    }


def result(
    parallelism: int, rate: float, read: float, trend: str, latency: float | None = None
) -> Row:
    """A results.csv row; latency is every processing percentile."""
    row = dict.fromkeys(RESULT_COLUMNS, "")
    return row | {
        "parallelism": str(parallelism),
        "partitions": "6",
        "rate": cell(rate),
        "read_per_s": cell(read),
        "lag_trend": trend,
        "processing_p50_s": cell(latency),
        "processing_p95_s": cell(latency),
        "processing_p99_s": cell(latency),
        "busiest_task": "deduplicate-events, to-event-rows",
        "busiest_task_busy_ms_per_s": "990",
    }


def test_running_a_rate_again_replaces_its_row() -> None:
    saved = [result(1, 100, 99, "steady"), result(1, 200, 150, "growing")]
    new = [result(1, 200, 160, "growing"), result(2, 100, 99, "steady")]

    merged = merge(saved, new)

    assert [(row["parallelism"], row["rate"], row["read_per_s"]) for row in merged] == [
        ("1", "100", "99"),
        ("1", "200", "160"),
        ("2", "100", "99"),
    ]


def test_runs_are_saved_to_csv_files(tmp_path: Path) -> None:
    tasks = {"viewing-events-source": TaskTime(300.0, 650.0)}
    save_run(tmp_path, result(1, 200, 150, "growing"), task_rows(RUN, tasks))
    save_run(tmp_path, result(1, 200, 155, "growing"), task_rows(RUN, tasks))

    assert [row["read_per_s"] for row in read_rows(tmp_path / "results.csv")] == ["155"]
    assert len(read_rows(tmp_path / "tasks.csv")) == 1


RESULTS = [
    result(1, 100, 99.5, "steady", 6.2),
    result(1, 150, 149.0, "steady", 7.0),
    result(1, 200, 151.0, "growing", 25.0),
    result(1, 500, 152.0, "steady"),  # noise above the breaking point
    result(2, 150, 149.5, "steady", 6.0),
    result(2, 300, 298.0, "shrinking", 6.5),
    result(2, 500, 310.0, "growing", 40.0),
]


def test_the_breaking_point_is_the_lowest_rate_whose_lag_grew() -> None:
    assert summarise(RESULTS) == [
        Summary(
            (1, 6), breaking_rate=200, sustained_rate=150, sustained_read_per_s=149
        ),
        Summary(
            (2, 6), breaking_rate=500, sustained_rate=300, sustained_read_per_s=298
        ),
    ]


def test_a_setup_that_always_kept_up_has_no_breaking_point() -> None:
    assert summarise([result(1, 100, 99.5, "steady")]) == [
        Summary((1, 6), None, 100, 99.5)
    ]


def test_results_print_as_a_markdown_table() -> None:
    lines = markdown_table(RESULTS)

    assert lines[0].startswith("| Parallelism | Partitions | Rate (events/s) |")
    assert lines[2] == (
        "| 1 | 6 | 100 | n/a | 100 | n/a | steady | n/a | 6.2 / 6.2 / 6.2 "
        "| n/a / n/a / n/a | deduplicate-events, to-event-rows (990) |"
    )
    one = (
        "- parallelism 1, 6 partitions: sustained 149 events/s "
        "(the 150 events/s run); lag grew from 200 events/s."
    )
    two = (
        "- parallelism 2, 6 partitions: sustained 298 events/s "
        "(the 300 events/s run); lag grew from 500 events/s."
    )
    assert lines[-2:] == [one, two]


def test_charts_are_drawn_from_the_saved_results(tmp_path: Path) -> None:
    for row in RESULTS:
        run = Run(int(row["parallelism"]), 6, float(row["rate"]), 120.0, 42)
        tasks = {
            "viewing-events-source": TaskTime(300.0, 650.0),
            "deduplicate-events, to-event-rows": TaskTime(990.0, 0.0),
        }
        save_run(tmp_path, row, task_rows(run, tasks))

    charts = draw_charts(tmp_path)

    assert [chart.name for chart in charts] == [
        "throughput.png",
        "latency.png",
        "busy.png",
    ]
    assert all(chart.read_bytes().startswith(b"\x89PNG") for chart in charts)


def test_no_saved_results_draw_no_charts(tmp_path: Path) -> None:
    assert draw_charts(tmp_path) == []


def test_a_single_rate_gets_the_table_without_charts(tmp_path: Path) -> None:
    save_run(tmp_path, result(1, 100, 99.5, "steady", 6.2), [])
    lines: list[str] = []

    report(tmp_path, say=lines.append)

    assert list(tmp_path.glob("*.png")) == []
    assert lines[0].startswith("| Parallelism | Partitions |")
