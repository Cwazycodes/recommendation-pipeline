from pathlib import Path

import pytest

from realtime_recommendation_pipeline.settings import (
    DynamoDBSettings,
    FlinkSettings,
    KafkaSettings,
    MetricsSettings,
    SchemaRegistrySettings,
)
from realtime_recommendation_pipeline.tools.bench import (
    Run,
    TaskTime,
    read_rows,
    write_rows,
)
from realtime_recommendation_pipeline.tools.bench_continue_watching import (
    RESULT_COLUMNS,
    between,
    main,
    markdown_table,
    percentile,
    result_row,
    run_bench,
)
from realtime_recommendation_pipeline.tools.demo import DemoError, DemoSettings
from realtime_recommendation_pipeline.tools.metrics_report import Ingest

ENV_EXAMPLE = Path(__file__).resolve().parents[1] / ".env.example"

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


def test_the_benchmark_refuses_dynamodb_in_aws(tmp_path: Path) -> None:
    settings = DemoSettings(
        KafkaSettings(_env_file=ENV_EXAMPLE),  # type: ignore[call-arg]
        FlinkSettings(_env_file=ENV_EXAMPLE),  # type: ignore[call-arg]
        # No endpoint: boto3 would use the table in AWS.
        DynamoDBSettings(_env_file=ENV_EXAMPLE, endpoint_url=None),  # type: ignore[call-arg]
        MetricsSettings(_env_file=ENV_EXAMPLE),  # type: ignore[call-arg]
        SchemaRegistrySettings(_env_file=ENV_EXAMPLE),  # type: ignore[call-arg]
    )

    with pytest.raises(DemoError, match="DynamoDB Local"):
        run_bench(settings, [RUN], tmp_path, say=lambda _line: None)

    assert list(tmp_path.iterdir()) == []


def test_report_only_prints_the_saved_results(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    row = result_row(
        RUN,
        started_at=1_793_000_000.0,
        ingest=INGEST,
        latency_s=[4.0, 5.0],
        write_s=[0.02],
        tasks={"track-progress": TaskTime(640.5, 0.0)},
    )
    write_rows(tmp_path / "results.csv", RESULT_COLUMNS, [row])

    main(["--report-only", "--out-dir", str(tmp_path)])

    saved = read_rows(tmp_path / "results.csv")
    assert capsys.readouterr().out.splitlines() == markdown_table(saved)
