"""Trending programmes: distinct viewers per sliding window, ranked top N."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass

from pyflink.common import Row
from pyflink.table import StreamTableEnvironment, Table


@dataclass(frozen=True)
class TrendingConfig:
    """HOP window size and slide in seconds, and how many programmes to rank."""

    window_size_seconds: int = 3600
    window_slide_seconds: int = 300
    top_n: int = 20

    def __post_init__(self) -> None:
        if min(self.window_size_seconds, self.window_slide_seconds, self.top_n) < 1:
            raise ValueError("window size, slide and top_n must be at least 1")

        if self.window_size_seconds % self.window_slide_seconds:
            raise ValueError("window size must be a whole multiple of the slide")


def seconds_interval(seconds: int) -> str:
    """SQL interval literal; SECOND alone only allows two digits (max 99)."""
    hours, rest = divmod(seconds, 3600)
    return f"INTERVAL '{hours}:{rest // 60:02d}:{rest % 60:02d}' HOUR TO SECOND"


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
                        {seconds_interval(config.window_slide_seconds)},
                        {seconds_interval(config.window_size_seconds)}
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
