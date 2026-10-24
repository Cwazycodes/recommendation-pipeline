import json

from pyflink.datastream.state import StateTtlConfig

from realtime_recommendation_pipeline.flink_jobs.dedup import (
    DeduplicateEvents,
    event_id_of,
    seen_ttl_config,
)
from realtime_recommendation_pipeline.producer.generate import generate_events


class FakeKeyedValueState:
    """ValueState stand-in that scopes values to the current key, like Flink."""

    def __init__(self) -> None:
        self.current_key: str | None = None
        self.values: dict[str | None, object] = {}

    def value(self) -> object:
        return self.values.get(self.current_key)

    def update(self, value: object) -> None:
        self.values[self.current_key] = value


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
        self.state = FakeKeyedValueState()
        self.metrics = FakeMetricGroup()

    def get_state(self, descriptor: object) -> FakeKeyedValueState:
        return self.state

    def get_metrics_group(self) -> FakeMetricGroup:
        return self.metrics


def run_dedup(events: list[str]) -> tuple[list[str], FakeRuntimeContext]:
    """Feed events through the function the way a keyed stream would."""
    function = DeduplicateEvents()
    runtime_context = FakeRuntimeContext()
    function.open(runtime_context)

    output: list[str] = []

    for event in events:
        runtime_context.state.current_key = event_id_of(event)
        output.extend(function.process_element(event, None))

    return output, runtime_context


def make_event(event_id: str) -> str:
    return json.dumps({"event_id": event_id, "user_id": "user-001"})


def test_event_id_of_reads_the_event_id() -> None:
    assert event_id_of(make_event("abc")) == "abc"


def test_first_copy_is_emitted() -> None:
    output, _ = run_dedup([make_event("a")])

    assert output == [make_event("a")]


def test_repeat_copies_are_dropped() -> None:
    output, _ = run_dedup([make_event("a"), make_event("a"), make_event("a")])

    assert output == [make_event("a")]


def test_different_event_ids_are_all_emitted() -> None:
    events = [make_event("a"), make_event("b"), make_event("a"), make_event("c")]

    output, _ = run_dedup(events)

    assert output == [make_event("a"), make_event("b"), make_event("c")]


def test_counters_track_unique_and_duplicate_events() -> None:
    _, runtime_context = run_dedup([make_event("a"), make_event("a"), make_event("b")])

    counters = runtime_context.metrics.counters
    assert counters["unique_events"].count == 2
    assert counters["duplicate_events"].count == 1


def test_dup_rate_run_matches_clean_run() -> None:
    clean = [json.dumps(event) for event in generate_events(1_000, seed=42)]
    with_dups = [
        json.dumps(event) for event in generate_events(1_000, seed=42, dup_rate=0.05)
    ]

    output, _ = run_dedup(with_dups)

    assert len(with_dups) > len(clean)
    assert output == clean


def test_seen_state_expires_one_hour_after_first_sighting() -> None:
    config = seen_ttl_config()

    assert config.is_enabled()
    assert config.get_ttl().to_milliseconds() == 60 * 60 * 1_000
    assert config.get_update_type() == StateTtlConfig.UpdateType.OnCreateAndWrite
