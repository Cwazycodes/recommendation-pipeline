import json
from datetime import UTC, datetime, timedelta

from pyflink.common import Types
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

from realtime_recommendation_pipeline.flink_jobs.event_time import (
    to_events_table,
    viewing_event_watermarks,
)

T0 = datetime(2026, 9, 21, 12, 0, tzinfo=UTC)


class EventTimeAssigner(TimestampAssigner):
    """Stands in for Kafka, where the producer stamps records with event_time."""

    def extract_timestamp(self, value: str, record_timestamp: int) -> int:
        return int(json.loads(value)["event_time"])


def make_event(event_id: str, seconds_after_t0: int) -> str:
    event_time = int((T0 + timedelta(seconds=seconds_after_t0)).timestamp() * 1_000)

    return json.dumps(
        {
            "event_id": event_id,
            "user_id": "user-001",
            "programme_id": "programme-001",
            "event_type": "heartbeat",
            "position_s": 30.0,
            "duration_s": 1800.0,
            "device": "smart_tv",
            "event_time": event_time,
            "produced_at": event_time + 25,
        }
    )


def one_minute_windows(events: list[str]) -> dict[datetime, list[str]]:
    """Run events through the watermarks and table, then count per minute."""
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    t_env = StreamTableEnvironment.create(env)
    t_env.get_config().set_local_timezone("UTC")

    stream = env.from_collection(
        events, type_info=Types.STRING()
    ).assign_timestamps_and_watermarks(
        viewing_event_watermarks().with_timestamp_assigner(EventTimeAssigner())
    )

    t_env.create_temporary_view("viewing_events", to_events_table(t_env, stream))

    result = t_env.sql_query(
        """
        SELECT window_start, LISTAGG(event_id) AS event_ids
        FROM TABLE(
            TUMBLE(TABLE viewing_events, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)
        )
        GROUP BY window_start, window_end
        """
    ).execute()

    with result.collect() as rows:
        return {row[0]: sorted(row[1].split(",")) for row in rows}


def test_event_time_is_the_rowtime_attribute() -> None:
    env = StreamExecutionEnvironment.get_execution_environment()
    t_env = StreamTableEnvironment.create(env)
    stream = env.from_collection([make_event("e1", 0)], type_info=Types.STRING())

    schema = str(to_events_table(t_env, stream).get_schema())

    assert "event_time: TIMESTAMP_LTZ(3) *ROWTIME*" in schema


def test_out_of_order_events_within_bound_land_in_correct_window() -> None:
    events = [
        make_event("e1", 10),  # 12:00:10
        make_event("e2", 50),  # 12:00:50
        make_event("e3", 70),  # 12:01:10, newest so far
        make_event("e4", 45),  # 12:00:45, arrives 25 s behind e3
        make_event("e5", 125),  # 12:02:05, newest so far
        make_event("e6", 100),  # 12:01:40, arrives 25 s behind e5
    ]

    windows = one_minute_windows(events)

    assert windows == {
        datetime(2026, 9, 21, 12, 0): ["e1", "e2", "e4"],
        datetime(2026, 9, 21, 12, 1): ["e3", "e6"],
        datetime(2026, 9, 21, 12, 2): ["e5"],
    }
