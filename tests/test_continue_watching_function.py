"""The continue-watching job's Flink functions, run without Flink.

Like test_dedup.py, the runtime context is a fake, so these run in make test.
test_continue_watching.py runs the same functions in a local Flink mini-cluster.
"""

import json
from typing import Any

from pyflink.datastream.state import StateTtlConfig

from realtime_recommendation_pipeline.common.event_schemas import (
    WriterSchemas,
    encode,
    parsed_schema,
    schema_text,
)
from realtime_recommendation_pipeline.continue_watching_oracle import (
    InProgress,
    continue_watching_oracle,
)
from realtime_recommendation_pipeline.flink_jobs.continue_watching import (
    ContinueWatching,
    ValidEvents,
    progress_ttl_config,
    user_id_of,
)
from realtime_recommendation_pipeline.producer.generate import generate_events


class FakeKeyedMapState:
    """MapState stand-in that scopes entries to the current key, like Flink."""

    def __init__(self) -> None:
        self.current_key: str | None = None
        self.entries: dict[tuple[str | None, str], str] = {}

    def get(self, key: str) -> str | None:
        return self.entries.get((self.current_key, key))

    def put(self, key: str, value: str) -> None:
        self.entries[(self.current_key, key)] = value


class FakeCounter:
    def __init__(self) -> None:
        self.count = 0

    def inc(self, n: int = 1) -> None:
        self.count += n


class FakeMetricGroup:
    def __init__(self) -> None:
        self.counters: dict[str, FakeCounter] = {}

    def counter(self, name: str) -> FakeCounter:
        return self.counters.setdefault(name, FakeCounter())


class FakeRuntimeContext:
    def __init__(self) -> None:
        self.state = FakeKeyedMapState()
        self.metrics = FakeMetricGroup()
        self.descriptor: Any = None

    def get_map_state(self, descriptor: Any) -> FakeKeyedMapState:
        self.descriptor = descriptor
        return self.state

    def get_metrics_group(self) -> FakeMetricGroup:
        return self.metrics


def run_keyed(
    events: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], FakeRuntimeContext]:
    """Feed events through the function the way a stream keyed by user_id would."""
    function = ContinueWatching()
    runtime_context = FakeRuntimeContext()
    function.open(runtime_context)

    changes: list[dict[str, Any]] = []
    for event in events:
        value = json.dumps(event)
        runtime_context.state.current_key = user_id_of(value)
        changes += [json.loads(c) for c in function.process_element(value, None)]

    return changes, runtime_context


def event(
    seconds: int,
    user_id: str,
    programme_id: str,
    position_s: float,
    event_id: str | None = None,
) -> dict[str, Any]:
    event_time = 1_790_000_000_000 + seconds * 1000
    return {
        "event_id": event_id or f"{user_id}-{programme_id}-{seconds}",
        "user_id": user_id,
        "programme_id": programme_id,
        "event_type": "heartbeat",
        "position_s": position_s,
        "duration_s": 1800.0,
        "device": "smart_tv",
        "event_time": event_time,
        "produced_at": event_time + 25,
    }


def test_user_id_of_reads_the_user_id() -> None:
    assert user_id_of(json.dumps(event(0, "u1", "prog-a", 0.0))) == "u1"


def test_changes_and_counters_follow_the_rule() -> None:
    changes, runtime_context = run_keyed(
        [
            event(0, "u1", "prog-a", 0.0),
            event(30, "u1", "prog-a", 30.0),
            event(0, "u2", "prog-a", 1700.0),  # finished before it was listed
            event(60, "u1", "prog-a", 1700.0),  # finished: removed
            event(45, "u1", "prog-a", 900.0),  # late, older than the finish
            event(60, "u1", "prog-a", 1700.0),  # repeated
        ]
    )

    assert [(c["op"], c["user_id"], c["position_s"]) for c in changes] == [
        ("upsert", "u1", 0.0),
        ("upsert", "u1", 30.0),
        ("delete", "u1", 1700.0),
    ]
    counters = runtime_context.metrics.counters
    assert counters["continue_watching_upserts"].count == 2
    assert counters["continue_watching_deletes"].count == 1
    assert counters["continue_watching_ignored_events"].count == 2


def test_state_keeps_the_newest_event_per_user_and_programme() -> None:
    _, runtime_context = run_keyed(
        [
            event(0, "u1", "prog-a", 0.0),
            event(30, "u1", "prog-a", 30.0),
            event(0, "u2", "prog-a", 1700.0),
        ]
    )

    stored = {
        key: json.loads(value) for key, value in runtime_context.state.entries.items()
    }
    assert stored == {
        ("u1", "prog-a"): {
            "event_time": 1_790_000_030_000,
            "event_id": "u1-prog-a-30",
            "position_s": 30.0,
            "duration_s": 1800.0,
            "finished": False,
        },
        # Kept, marked finished, so an older event cannot list it.
        ("u2", "prog-a"): {
            "event_time": 1_790_000_000_000,
            "event_id": "u2-prog-a-0",
            "position_s": 1700.0,
            "duration_s": 1800.0,
            "finished": True,
        },
    }


def test_applying_the_changes_matches_the_oracle() -> None:
    events = generate_events(2_000, seed=7, dup_rate=0.05, late_rate=0.02)

    changes, _ = run_keyed(events)

    listed: dict[tuple[str, str], InProgress] = {}
    for c in changes:
        key = (c["user_id"], c["programme_id"])
        if c["op"] == "upsert":
            listed[key] = InProgress(
                c["user_id"],
                c["programme_id"],
                c["position_s"],
                c["duration_s"],
                c["event_time"],
            )
        else:
            del listed[key]
    assert sorted(listed.values()) == continue_watching_oracle(events)


def test_progress_state_expires_30_days_after_its_last_update() -> None:
    config = progress_ttl_config()

    assert config.is_enabled()
    assert config.get_ttl().to_milliseconds() == 30 * 24 * 60 * 60 * 1_000
    assert config.get_update_type() == StateTtlConfig.UpdateType.OnCreateAndWrite
    assert (
        config.get_state_visibility()
        == StateTtlConfig.StateVisibility.NeverReturnExpired
    )


def test_the_function_asks_for_one_map_state_called_progress() -> None:
    _, runtime_context = run_keyed([])

    assert runtime_context.descriptor.get_name() == "progress"


def test_valid_events_pass_and_invalid_ones_are_counted() -> None:
    # The job's events come from the producer, in Avro, with a UUID event_id.
    (generated,) = generate_events(1, seed=1)
    valid = encode(generated, parsed_schema(2), 7)
    function = ValidEvents(WriterSchemas({7: schema_text(2)}.__getitem__))
    runtime_context = FakeRuntimeContext()
    function.open(runtime_context)

    # The Kafka source hands the function each value's bytes as ISO-8859-1.
    output = [
        out
        for value in [valid, b"not avro", valid[:20]]
        for out in function.flat_map(value.decode("ISO-8859-1"))
    ]

    assert [json.loads(out)["event_id"] for out in output] == [generated["event_id"]]
    counters = runtime_context.metrics.counters
    assert counters["valid_events"].count == 1
    assert counters["invalid_events"].count == 2
