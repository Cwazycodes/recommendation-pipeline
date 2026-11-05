"""Arguments for the streaming job in jobs/validate_events.py."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from dataclasses import dataclass, field

from realtime_recommendation_pipeline.flink_jobs.trending import TrendingConfig


@dataclass(frozen=True)
class JobConfig:
    """The topics the job reads and writes, and its trending settings."""

    trending: TrendingConfig = field(default_factory=TrendingConfig)
    source_topic: str = "viewing-events"
    trending_topic: str = "trending-results"
    dlq_topic: str = "viewing-events-dlq"


def parse_job_config(argv: Sequence[str] | None = None) -> JobConfig:
    """Read the job's settings from its arguments; each one has a default."""
    defaults = JobConfig()
    parser = argparse.ArgumentParser(
        description="Validate, deduplicate and rank viewing events."
    )
    parser.add_argument(
        "--window-size-seconds", type=int, default=defaults.trending.window_size_seconds
    )
    parser.add_argument(
        "--window-slide-seconds",
        type=int,
        default=defaults.trending.window_slide_seconds,
    )
    parser.add_argument("--top-n", type=int, default=defaults.trending.top_n)
    parser.add_argument("--source-topic", default=defaults.source_topic)
    parser.add_argument("--trending-topic", default=defaults.trending_topic)
    parser.add_argument("--dlq-topic", default=defaults.dlq_topic)
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
