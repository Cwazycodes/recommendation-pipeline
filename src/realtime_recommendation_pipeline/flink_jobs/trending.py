"""Trending programmes: distinct viewers per sliding window, ranked top N."""

from __future__ import annotations

import argparse
import json
import time
from collections.abc import Sequence
from dataclasses import dataclass

from pyflink.common import Row
from pyflink.table import StreamTableEnvironment, Table


@dataclass(frozen=True)
class TrendingConfig:
    """HOP window size and slide, and how many programmes to rank."""

    window_size_minutes: int = 60
    window_slide_minutes: int = 5
    top_n: int = 20

    def __post_init__(self) -> None:
        if min(self.window_size_minutes, self.window_slide_minutes, self.top_n) < 1:
            raise ValueError("window size, slide and top_n must be at least 1")

        if self.window_size_minutes % self.window_slide_minutes:
            raise ValueError("window size must be a whole multiple of the slide")


def parse_trending_config(argv: Sequence[str] | None = None) -> TrendingConfig:
    """Read the trending settings from job arguments."""
    parser = argparse.ArgumentParser(description="Trending programmes job.")
    parser.add_argument("--window-size-minutes", type=int, default=60)
    parser.add_argument("--window-slide-minutes", type=int, default=5)
    parser.add_argument("--top-n", type=int, default=20)
    args = parser.parse_args(argv)

    return TrendingConfig(
        window_size_minutes=args.window_size_minutes,
        window_slide_minutes=args.window_slide_minutes,
        top_n=args.top_n,
    )


def minutes_interval(minutes: int) -> str:
    """SQL interval literal; MINUTE alone only allows two digits (max 99)."""
    return f"INTERVAL '{minutes // 60}:{minutes % 60:02d}' HOUR TO MINUTE"


def trending_sql(view: str, config: TrendingConfig) -> str:
    """Window Top-N over HOP windows, scored by distinct viewers."""
    return f"""
        SELECT window_start, window_end, `rank`, programme_id, score, max_produced_at
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY window_start, window_end
                    ORDER BY score DESC, programme_id
                ) AS `rank`
            FROM (
                SELECT
                    window_start,
                    window_end,
                    programme_id,
                    COUNT(DISTINCT user_id) AS score,
                    MAX(produced_at) AS max_produced_at
                FROM TABLE(
                    HOP(
                        TABLE {view},
                        DESCRIPTOR(event_time),
                        {minutes_interval(config.window_slide_minutes)},
                        {minutes_interval(config.window_size_minutes)}
                    )
                )
                WHERE event_type IN ('play_start', 'heartbeat')
                GROUP BY window_start, window_end, programme_id
            )
        )
        WHERE `rank` <= {config.top_n}
    """


def trending_table(
    t_env: StreamTableEnvironment, events: Table, config: TrendingConfig
) -> Table:
    """Ranked trending rows; t_env must use UTC as its local time zone."""
    t_env.create_temporary_view("viewing_events", events)

    return t_env.sql_query(trending_sql("viewing_events", config))


def to_trending_json(row: Row) -> str:
    """Kafka message for one ranked programme, stamped with emitted_at."""
    return json.dumps(
        {
            "window_start": row.window_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "window_end": row.window_end.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "rank": row.rank,
            "programme_id": row.programme_id,
            "score": row.score,
            "max_produced_at": row.max_produced_at,
            "emitted_at": time.time_ns() // 1_000_000,
        }
    )
