import pytest

from realtime_recommendation_pipeline.flink_jobs.job_config import (
    JobConfig,
    parse_job_config,
)
from realtime_recommendation_pipeline.flink_jobs.trending import TrendingConfig
from realtime_recommendation_pipeline.settings import KafkaSettings

KAFKA = KafkaSettings(
    bootstrap_servers="localhost:9092",
    events_topic="viewing-events",
    events_dlq_topic="viewing-events-dlq",
    trending_topic="trending-results",
    writer_dlq_topic="trending-writer-dlq",
    writer_group_id="results-writer",
    writer_instance_id="results-writer-1",
)


def test_no_arguments_gives_the_production_job_on_the_configured_topics() -> None:
    assert parse_job_config(KAFKA, []) == JobConfig(
        trending=TrendingConfig(
            window_size_seconds=3600, window_slide_seconds=300, top_n=20
        ),
        source_topic="viewing-events",
        trending_topic="trending-results",
        dlq_topic="viewing-events-dlq",
    )


def test_demo_settings_come_from_job_arguments() -> None:
    config = parse_job_config(
        KAFKA,
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


def test_parallelism_is_a_job_argument_and_one_by_default() -> None:
    assert parse_job_config(KAFKA, []).parallelism == 1
    assert parse_job_config(KAFKA, ["--parallelism", "2"]).parallelism == 2


def test_parallelism_must_be_at_least_one() -> None:
    with pytest.raises(SystemExit):
        parse_job_config(KAFKA, ["--parallelism", "0"])
