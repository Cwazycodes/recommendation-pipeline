import json
import time
from datetime import UTC, datetime, timedelta

import pytest
from pyflink.common import Row, Types
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

from realtime_recommendation_pipeline.flink_jobs.event_time import (
    to_events_table,
    viewing_event_watermarks,
)
from realtime_recommendation_pipeline.flink_jobs.trending import (
    TrendingConfig,
    minutes_interval,
    parse_trending_config,
    to_trending_json,
    trending_table,
)

T0 = datetime(2026, 9, 21, 12, 0, tzinfo=UTC)


class EventTimeAssigner(TimestampAssigner):
    """Stands in for Kafka, where the producer stamps records with event_time."""

    def extract_timestamp(self, value: str, record_timestamp: int) -> int:
        return int(json.loads(value)["event_time"])


def make_event(
    seconds_after_t0: int, user_id: str, programme_id: str, event_type: str
) -> str:
    event_time = int((T0 + timedelta(seconds=seconds_after_t0)).timestamp() * 1_000)

    return json.dumps(
        {
            "event_id": f"{user_id}-{programme_id}-{seconds_after_t0}",
            "user_id": user_id,
            "programme_id": programme_id,
            "event_type": event_type,
            "position_s": 0.0,
            "duration_s": 1800.0,
            "device": "smart_tv",
            "event_time": event_time,
            "produced_at": event_time + 25,
        }
    )


def run_trending(events: list[str], config: TrendingConfig) -> list[tuple]:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    t_env = StreamTableEnvironment.create(env)
    t_env.get_config().set_local_timezone("UTC")

    stream = env.from_collection(
        events, type_info=Types.STRING()
    ).assign_timestamps_and_watermarks(
        viewing_event_watermarks().with_timestamp_assigner(EventTimeAssigner())
    )

    table = trending_table(t_env, to_events_table(t_env, stream), config)

    # Collected rows are positional: window_start, window_end, rank,
    # programme_id, score, max_produced_at.
    with table.execute().collect() as rows:
        return sorted(
            (row[0].strftime("%H:%M"), row[1].strftime("%H:%M"), *row[2:5])
            for row in rows
        )


def test_defaults_match_the_ticket() -> None:
    assert TrendingConfig() == TrendingConfig(
        window_size_minutes=60, window_slide_minutes=5, top_n=20
    )


def test_settings_come_from_job_arguments() -> None:
    config = parse_trending_config(["--window-size-minutes", "30", "--top-n", "5"])

    assert config == TrendingConfig(
        window_size_minutes=30, window_slide_minutes=5, top_n=5
    )


@pytest.mark.parametrize(
    "settings",
    [{"window_slide_minutes": 7}, {"window_size_minutes": 0}, {"top_n": 0}],
)
def test_invalid_settings_are_rejected(settings: dict[str, int]) -> None:
    with pytest.raises(ValueError):
        TrendingConfig(**settings)


def test_minutes_interval_handles_more_than_99_minutes() -> None:
    assert minutes_interval(5) == "INTERVAL '0:05' HOUR TO MINUTE"
    assert minutes_interval(120) == "INTERVAL '2:00' HOUR TO MINUTE"


def test_trending_json_has_the_ticket_fields() -> None:
    row = Row(
        window_start=datetime(2026, 9, 21, 12, 0),
        window_end=datetime(2026, 9, 21, 13, 0),
        rank=1,
        programme_id="programme-0001",
        score=12,
        max_produced_at=1_790_000_000_025,
    )

    message = json.loads(to_trending_json(row))

    assert message["window_start"] == "2026-09-21T12:00:00Z"
    assert message["window_end"] == "2026-09-21T13:00:00Z"
    assert message["rank"] == 1
    assert message["score"] == 12
    assert message["max_produced_at"] == 1_790_000_000_025
    assert abs(message["emitted_at"] - time.time_ns() // 1_000_000) < 5_000


@pytest.mark.slow
def test_ranks_distinct_viewers_per_sliding_window() -> None:
    events = [
        make_event(60, "user-1", "prog-a", "play_start"),
        make_event(90, "user-1", "prog-a", "heartbeat"),
        make_event(100, "user-2", "prog-b", "heartbeat"),
        make_event(120, "user-1", "prog-a", "heartbeat"),
        make_event(130, "user-3", "prog-b", "heartbeat"),
        make_event(140, "user-4", "prog-c", "pause"),
        make_event(150, "user-1", "prog-a", "heartbeat"),
        make_event(160, "user-5", "prog-c", "play_end"),
        make_event(170, "user-6", "prog-d", "heartbeat"),
        make_event(420, "user-7", "prog-e", "play_start"),
        make_event(430, "user-8", "prog-e", "heartbeat"),
        make_event(440, "user-9", "prog-e", "heartbeat"),
    ]
    config = TrendingConfig(window_size_minutes=10, window_slide_minutes=5, top_n=2)

    rows = run_trending(events, config)

    # user-1 spams prog-a but scores 1; prog-c only has pause/play_end;
    # prog-d ties prog-a on 1 and loses on programme_id, so top 2 drops it.
    assert rows == [
        ("11:55", "12:05", 1, "prog-b", 2),
        ("11:55", "12:05", 2, "prog-a", 1),
        ("12:00", "12:10", 1, "prog-e", 3),
        ("12:00", "12:10", 2, "prog-b", 2),
        ("12:05", "12:15", 1, "prog-e", 3),
    ]
