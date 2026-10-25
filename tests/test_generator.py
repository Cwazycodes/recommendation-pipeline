import json

from realtime_recommendation_pipeline.producer.generate import (
    BASE_TIME_MS,
    generate_events,
    generate_valid_events,
)


def test_same_seed_produces_identical_events() -> None:
    first = generate_events(100, seed=42)
    second = generate_events(100, seed=42)

    assert first == second


def test_different_seeds_produce_different_events() -> None:
    first = generate_events(100, seed=42)
    second = generate_events(100, seed=99)

    assert first != second


def test_valid_events_have_required_fields() -> None:
    events = generate_valid_events(100, seed=42)

    required_fields = {
        "event_id",
        "schema_version",
        "user_id",
        "programme_id",
        "event_type",
        "position_s",
        "duration_s",
        "device",
        "event_time",
        "produced_at",
    }

    for event in events:
        assert set(event) == required_fields


def test_duplicate_rate_one_duplicates_every_event() -> None:
    events = generate_events(
        100,
        seed=42,
        dup_rate=1.0,
    )

    assert len(events) == 200

    event_ids = [event["event_id"] for event in events]

    assert len(set(event_ids)) == 100


def test_malformed_rate_one_makes_every_event_invalid() -> None:
    events = generate_events(
        100,
        seed=42,
        malformed_rate=1.0,
    )

    assert all(event["event_time"] == "not-a-timestamp" for event in events)


def test_jsonl_output_is_parseable() -> None:
    events = generate_events(10, seed=42)

    lines = [json.dumps(event) for event in events]

    assert len(lines) == 10

    for line in lines:
        parsed = json.loads(line)
        assert parsed["schema_version"] == 1


def test_late_events_are_emitted_out_of_order() -> None:
    normal = generate_events(100, seed=42)

    late = generate_events(
        100,
        seed=42,
        late_rate=0.5,
    )

    normal_times = [event["event_time"] for event in normal]
    late_times = [event["event_time"] for event in late]

    assert sorted(normal_times) == sorted(late_times)
    assert normal_times != late_times


def test_valid_events_are_in_event_time_order() -> None:
    events = generate_valid_events(1_000, seed=42)

    event_times = [event["event_time"] for event in events]

    assert event_times == sorted(event_times)


def test_sessions_overlap_so_each_hour_has_many_viewers() -> None:
    events = generate_valid_events(5_000, seed=42)

    first_hour_viewers = {
        event["user_id"]
        for event in events
        if event["event_time"] < BASE_TIME_MS + 3_600_000
    }

    assert len(first_hour_viewers) >= 30
