from realtime_recommendation_pipeline.flink_jobs.job_config import (
    JobConfig,
    parse_job_config,
)
from realtime_recommendation_pipeline.flink_jobs.trending import TrendingConfig


def test_no_arguments_gives_the_production_job() -> None:
    assert parse_job_config([]) == JobConfig(
        trending=TrendingConfig(
            window_size_seconds=3600, window_slide_seconds=300, top_n=20
        ),
        source_topic="viewing-events",
        trending_topic="trending-results",
        dlq_topic="viewing-events-dlq",
    )


def test_demo_settings_come_from_job_arguments() -> None:
    config = parse_job_config(
        [
            "--window-size-seconds", "120",
            "--window-slide-seconds", "10",
            "--top-n", "5",
            "--source-topic", "viewing-events-demo",
            "--trending-topic", "trending-results-demo",
            "--dlq-topic", "viewing-events-dlq-demo",
        ]
    )  # fmt: skip

    assert config == JobConfig(
        trending=TrendingConfig(
            window_size_seconds=120, window_slide_seconds=10, top_n=5
        ),
        source_topic="viewing-events-demo",
        trending_topic="trending-results-demo",
        dlq_topic="viewing-events-dlq-demo",
    )
