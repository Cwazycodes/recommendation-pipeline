"""Arguments for the streaming job in jobs/validate_events.py."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from dataclasses import dataclass

from realtime_recommendation_pipeline.flink_jobs.trending import TrendingConfig
from realtime_recommendation_pipeline.settings import KafkaSettings


@dataclass(frozen=True)
class JobConfig:
    """The topics the job reads and writes, and its trending settings."""

    trending: TrendingConfig
    source_topic: str
    trending_topic: str
    dlq_topic: str


def parse_job_config(
    kafka: KafkaSettings, argv: Sequence[str] | None = None
) -> JobConfig:
    """Read the job's arguments. Topics default to the KAFKA_* settings."""
    defaults = TrendingConfig()
    parser = argparse.ArgumentParser(
        description="Validate, deduplicate and rank viewing events."
    )
    parser.add_argument(
        "--window-size-seconds", type=int, default=defaults.window_size_seconds
    )
    parser.add_argument(
        "--window-slide-seconds",
        type=int,
        default=defaults.window_slide_seconds,
    )
    parser.add_argument("--top-n", type=int, default=defaults.top_n)
    parser.add_argument("--source-topic", default=kafka.events_topic)
    parser.add_argument("--trending-topic", default=kafka.trending_topic)
    parser.add_argument("--dlq-topic", default=kafka.events_dlq_topic)
    args = parser.parse_args(argv)

    return JobConfig(
        trending=TrendingConfig(
            window_size_seconds=args.window_size_seconds,
            window_slide_seconds=args.window_slide_seconds,
            top_n=args.top_n,
        ),
        source_topic=args.source_topic,
        trending_topic=args.trending_topic,
        dlq_topic=args.dlq_topic,
    )
