import json
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from pyflink.common import Types, WatermarkStrategy
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

from realtime_recommendation_pipeline.flink_jobs.dedup import deduplicate
from realtime_recommendation_pipeline.flink_jobs.event_time import (
    MAX_OUT_OF_ORDERNESS_S,
    to_events_table,
)
from realtime_recommendation_pipeline.flink_jobs.trending import (
    TrendingConfig,
    trending_table,
)
from realtime_recommendation_pipeline.producer.generate import generate_events
from realtime_recommendation_pipeline.trending_oracle import (
    TrendingRow,
    trending_oracle,
    windows_containing,
)

T0 = datetime(2026, 9, 21, 12, 0, tzinfo=UTC)

# 10-minute windows every 5 minutes, top 3: small enough to reason about.
SMALL = TrendingConfig(window_size_minutes=10, window_slide_minutes=5, top_n=3)

# The job's watermarks come from the stream every 200 ms, and a bounded test
# source finishes before the first one. A SQL watermark is emitted as soon as it
# moves more than the auto-watermark interval; at 0 that is after every record,
# so which events are late never depends on timing.
PER_RECORD_WATERMARK = f"event_time - INTERVAL '{MAX_OUT_OF_ORDERNESS_S}' SECOND"


def at(seconds_after_t0: int) -> int:
    return int((T0 + timedelta(seconds=seconds_after_t0)).timestamp() * 1_000)


def event(
    seconds_after_t0: int,
    user_id: str,
    programme_id: str,
    event_type: str = "heartbeat",
    event_id: str | None = None,
) -> dict[str, Any]:
    event_time = at(seconds_after_t0)

    return {
        "event_id": event_id or f"{user_id}-{programme_id}-{seconds_after_t0}",
        "user_id": user_id,
        "programme_id": programme_id,
        "event_type": event_type,
        "position_s": 0.0,
        "duration_s": 1800.0,
        "device": "smart_tv",
        "event_time": event_time,
        "produced_at": event_time + 25,
    }


CASES: dict[str, tuple[list[dict[str, Any]], TrendingConfig]] = {
    # Four programmes on 2 viewers: programme_id breaks the tie, top 3 cuts one.
    "ties": (
        [
            event(60, "u1", "prog-c"),
            event(65, "u2", "prog-c"),
            event(70, "u3", "prog-a"),
            event(75, "u4", "prog-a"),
            event(80, "u5", "prog-b"),
            event(85, "u6", "prog-b"),
            event(90, "u7", "prog-d"),
            event(95, "u8", "prog-e"),
            event(100, "u9", "prog-e"),
        ],
        SMALL,
    ),
    "late_events": (
        [
            event(60, "u1", "prog-a"),
            event(400, "u2", "prog-a"),  # watermark -> 12:06:10, fires 11:55-12:05
            event(380, "u3", "prog-b"),  # 20 s behind: inside the bound
            # 200 s behind: too late for 11:55-12:05, still counts in 12:00-12:10
            event(200, "u4", "prog-c"),
            event(900, "u5", "prog-a"),  # watermark -> 12:14:30, fires 12:00-12:10
            event(290, "u6", "prog-d"),  # both its windows fired: dropped
            event(870, "u7", "prog-b"),  # exactly on the watermark, window still open
        ],
        SMALL,
    ),
    "duplicates": (
        [
            event(60, "u1", "prog-a", event_id="a-1"),
            event(60, "u1", "prog-a", event_id="a-1"),
            event(60, "u1", "prog-a", event_id="a-1"),
            event(70, "u2", "prog-b", event_id="b-1"),
            event(80, "u3", "prog-b", event_id="b-1"),  # reused id: first copy wins
            event(90, "u4", "prog-c"),
        ],
        SMALL,
    ),
    "empty_windows": (
        [
            event(60, "u1", "prog-a"),
            event(2_000, "u2", "prog-b"),  # 33 minutes later: nothing in between
            event(2_010, "u3", "prog-c", event_type="pause"),
            event(2_020, "u4", "prog-c", event_type="play_end"),
            event(3_600, "u5", "prog-d", event_type="pause"),  # only unscored events
            event(3_900, "u6", "prog-e"),
        ],
        SMALL,
    ),
    "generated": (
        generate_events(2_000, seed=42, dup_rate=0.05, late_rate=0.02),
        TrendingConfig(),
    ),
}


def oracle(events: list[dict[str, Any]], config: TrendingConfig) -> list[TrendingRow]:
    return trending_oracle(
        events,
        window_size_ms=config.window_size_minutes * 60_000,
        window_slide_ms=config.window_slide_minutes * 60_000,
        top_n=config.top_n,
        max_out_of_orderness_ms=MAX_OUT_OF_ORDERNESS_S * 1_000,
    )


def ranked(rows: list[TrendingRow]) -> dict[str, list[tuple[int, str, int]]]:
    """Rows grouped by window, e.g. {"12:00-12:10": [(1, "prog-a", 2)]}."""

    def clock(ms: int) -> str:
        return datetime.fromtimestamp(ms / 1_000, UTC).strftime("%H:%M")

    windows: defaultdict[str, list[tuple[int, str, int]]] = defaultdict(list)
    for row in rows:
        label = f"{clock(row.window_start)}-{clock(row.window_end)}"
        windows[label].append((row.rank, row.programme_id, row.score))

    return dict(windows)


class EventTimeAssigner(TimestampAssigner):
    """Stands in for Kafka, where the producer stamps records with event_time."""

    def extract_timestamp(self, value: str, record_timestamp: int) -> int:
        return int(json.loads(value)["event_time"])


def run_flink(
    events: list[dict[str, Any]], config: TrendingConfig
) -> list[TrendingRow]:
    """The job's dedup, rowtime table and trending SQL on an in-memory source."""
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.get_config().set_auto_watermark_interval(0)

    t_env = StreamTableEnvironment.create(env)
    t_env.get_config().set_local_timezone("UTC")

    stream = env.from_collection(
        [json.dumps(event) for event in events], type_info=Types.STRING()
    ).assign_timestamps_and_watermarks(
        WatermarkStrategy.no_watermarks().with_timestamp_assigner(EventTimeAssigner())
    )

    table = to_events_table(t_env, deduplicate(stream), watermark=PER_RECORD_WATERMARK)

    def epoch_ms(moment: datetime) -> int:
        return int(moment.replace(tzinfo=UTC).timestamp() * 1_000)

    with trending_table(t_env, table, config).execute().collect() as rows:
        return sorted(
            TrendingRow(
                window_start=epoch_ms(row[0]),
                window_end=epoch_ms(row[1]),
                rank=row[2],
                programme_id=row[3],
                score=row[4],
                max_produced_at=row[5],
            )
            for row in rows
        )


def test_windows_containing_lists_every_sliding_window() -> None:
    windows = windows_containing(at(60), size_ms=600_000, slide_ms=300_000)

    assert windows == [(at(0), at(600)), (at(-300), at(300))]


def test_oracle_breaks_ties_by_programme_id() -> None:
    events, config = CASES["ties"]

    windows = ranked(oracle(events, config))

    assert windows["11:55-12:05"] == [
        (1, "prog-a", 2),
        (2, "prog-b", 2),
        (3, "prog-c", 2),
    ]


def test_oracle_counts_late_events_only_in_windows_still_open() -> None:
    events, config = CASES["late_events"]

    windows = ranked(oracle(events, config))

    assert windows == {
        "11:55-12:05": [(1, "prog-a", 1)],
        "12:00-12:10": [(1, "prog-a", 2), (2, "prog-b", 1), (3, "prog-c", 1)],
        "12:05-12:15": [(1, "prog-b", 2), (2, "prog-a", 1)],
        "12:10-12:20": [(1, "prog-a", 1), (2, "prog-b", 1)],
        "12:15-12:25": [(1, "prog-a", 1)],
    }


def test_oracle_keeps_the_first_copy_of_each_event_id() -> None:
    events, config = CASES["duplicates"]

    windows = ranked(oracle(events, config))

    assert windows["12:00-12:10"] == [
        (1, "prog-a", 1),
        (2, "prog-b", 1),
        (3, "prog-c", 1),
    ]


def test_oracle_emits_nothing_for_empty_windows() -> None:
    events, config = CASES["empty_windows"]

    windows = ranked(oracle(events, config))

    assert windows == {
        "11:55-12:05": [(1, "prog-a", 1)],
        "12:00-12:10": [(1, "prog-a", 1)],
        "12:25-12:35": [(1, "prog-b", 1)],
        "12:30-12:40": [(1, "prog-b", 1)],
        "13:00-13:10": [(1, "prog-e", 1)],
        "13:05-13:15": [(1, "prog-e", 1)],
    }


@pytest.mark.slow
@pytest.mark.parametrize("case", sorted(CASES))
def test_flink_matches_the_oracle(case: str) -> None:
    events, config = CASES[case]

    expected = oracle(events, config)

    assert expected, "every fixture should produce some trending rows"
    assert run_flink(events, config) == expected
