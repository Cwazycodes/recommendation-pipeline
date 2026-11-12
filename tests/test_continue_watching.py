import json
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment

from realtime_recommendation_pipeline.continue_watching_oracle import (
    InProgress,
    continue_watching_oracle,
)
from realtime_recommendation_pipeline.flink_jobs.continue_watching import (
    DELETE,
    UPSERT,
    Progress,
    advance,
    continue_watching,
    is_finished,
    to_change_json,
)
from realtime_recommendation_pipeline.producer.generate import generate_events

T0 = datetime(2026, 9, 21, 12, 0, tzinfo=UTC)


def at(seconds_after_t0: int) -> int:
    return int((T0 + timedelta(seconds=seconds_after_t0)).timestamp() * 1_000)


def event(
    seconds_after_t0: int,
    user_id: str,
    programme_id: str,
    position_s: float,
    duration_s: float | None = 1800.0,
    event_id: str | None = None,
    event_type: str = "heartbeat",
) -> dict[str, Any]:
    event_time = at(seconds_after_t0)

    return {
        "event_id": event_id or f"{user_id}-{programme_id}-{seconds_after_t0}",
        "user_id": user_id,
        "programme_id": programme_id,
        "event_type": event_type,
        "position_s": position_s,
        "duration_s": duration_s,
        "device": "smart_tv",
        "event_time": event_time,
        "produced_at": event_time + 25,
    }


def progress_of(source: dict[str, Any]) -> Progress:
    return Progress(
        event_time=source["event_time"],
        event_id=source["event_id"],
        position_s=source["position_s"],
        duration_s=source["duration_s"],
        finished=is_finished(source["position_s"], source["duration_s"]),
    )


CASES: dict[str, list[dict[str, Any]]] = {
    "in_progress": [
        event(0, "u1", "prog-a", 0.0, event_type="play_start"),
        event(30, "u1", "prog-a", 30.0),
        event(60, "u2", "prog-a", 0.0, event_type="play_start"),
        event(60, "u1", "prog-a", 60.0),
        event(90, "u1", "prog-b", 0.0, event_type="play_start"),
        event(120, "u2", "prog-a", 60.0, event_type="pause"),
    ],
    # 1800 s programmes: more than 1620 s watched is finished.
    "finished": [
        event(0, "u1", "prog-a", 1500.0),
        event(30, "u1", "prog-a", 1700.0),  # 94%: off the list
        event(0, "u1", "prog-b", 1590.0),
        event(30, "u1", "prog-b", 1620.0),  # exactly 90%: still on it
        event(0, "u2", "prog-c", 1600.0),
        event(30, "u2", "prog-c", 1625.0, event_type="play_end"),  # 90.3%
        event(60, "u2", "prog-c", 1655.0),  # still finished: nothing to remove
    ],
    "late_and_repeated": [
        event(0, "u1", "prog-a", 0.0, event_id="a-0"),
        event(60, "u1", "prog-a", 60.0, event_id="a-60"),
        event(30, "u1", "prog-a", 30.0, event_id="a-30"),  # late: older position
        event(60, "u1", "prog-a", 60.0, event_id="a-60"),  # repeated
        event(10, "u1", "prog-a", 900.0, event_id="a-60"),  # reused id, older
        event(60, "u2", "prog-b", 120.0, event_id="b-2"),
        event(60, "u2", "prog-b", 90.0, event_id="b-1"),  # same time: id decides
    ],
    "watched_again": [
        event(0, "u1", "prog-a", 1700.0),  # finished before it was ever listed
        event(0, "u1", "prog-b", 600.0),
        event(30, "u1", "prog-b", 1700.0),  # finished
        event(20, "u1", "prog-b", 900.0),  # late, older than the finish: ignored
        event(3_600, "u1", "prog-b", 0.0, event_type="play_start"),  # again
        event(3_630, "u1", "prog-b", 30.0),
    ],
    "unknown_duration": [
        event(0, "u1", "live-news", 5_000.0, duration_s=None),
        event(0, "u1", "trailer", 50.0, duration_s=0.0),
    ],
    "generated": generate_events(2_000, seed=42, dup_rate=0.05, late_rate=0.02),
}


def oracle_by_key(events: list[dict[str, Any]]) -> dict[tuple[str, str], InProgress]:
    return {
        (row.user_id, row.programme_id): row for row in continue_watching_oracle(events)
    }


# --- The job's rule, one event at a time ---------------------------------------


def test_first_event_lists_the_programme() -> None:
    first = event(0, "u1", "prog-a", 0.0, event_type="play_start")

    assert advance(None, first) == (progress_of(first), UPSERT)


def test_newer_event_moves_the_position() -> None:
    first = event(0, "u1", "prog-a", 0.0)
    newer = event(30, "u1", "prog-a", 30.0)

    assert advance(progress_of(first), newer) == (progress_of(newer), UPSERT)


@pytest.mark.parametrize(
    "arriving",
    [
        event(30, "u1", "prog-a", 30.0, event_id="older"),
        event(60, "u1", "prog-a", 60.0, event_id="kept"),
    ],
    ids=["older", "repeated"],
)
def test_event_no_newer_than_the_kept_one_changes_nothing(
    arriving: dict[str, Any],
) -> None:
    kept = progress_of(event(60, "u1", "prog-a", 60.0, event_id="kept"))

    progress, change = advance(kept, arriving)

    assert progress is kept
    assert change is None


def test_events_at_the_same_time_are_ordered_by_event_id() -> None:
    kept = progress_of(event(60, "u1", "prog-a", 60.0, event_id="b"))

    assert advance(kept, event(60, "u1", "prog-a", 90.0, event_id="a"))[1] is None
    assert advance(kept, event(60, "u1", "prog-a", 90.0, event_id="c"))[1] == UPSERT


def test_finishing_a_listed_programme_deletes_it_once() -> None:
    listed = progress_of(event(0, "u1", "prog-a", 1500.0))

    finished, change = advance(listed, event(30, "u1", "prog-a", 1700.0))
    assert finished.finished
    assert change == DELETE

    _, again = advance(finished, event(60, "u1", "prog-a", 1750.0))
    assert again is None


def test_exactly_90_percent_is_still_in_progress() -> None:
    assert not is_finished(1620.0, 1800.0)
    assert is_finished(1620.5, 1800.0)
    assert advance(None, event(0, "u1", "prog-a", 1620.0))[1] == UPSERT


def test_a_programme_finished_before_it_was_listed_publishes_nothing() -> None:
    progress, change = advance(None, event(0, "u1", "prog-a", 1700.0))

    assert progress.finished
    assert change is None


def test_an_older_event_cannot_bring_back_a_finished_programme() -> None:
    finished = progress_of(event(30, "u1", "prog-a", 1700.0))

    progress, change = advance(finished, event(20, "u1", "prog-a", 900.0))

    assert progress is finished
    assert change is None


def test_watching_again_after_finishing_lists_it_again() -> None:
    finished = progress_of(event(30, "u1", "prog-a", 1700.0))
    again = event(3_600, "u1", "prog-a", 0.0, event_type="play_start")

    assert advance(finished, again) == (progress_of(again), UPSERT)


@pytest.mark.parametrize("duration_s", [None, 0.0])
def test_without_a_duration_nothing_counts_as_finished(
    duration_s: float | None,
) -> None:
    assert advance(None, event(0, "u1", "prog-a", 9_999.0, duration_s))[1] == UPSERT


def test_change_carries_the_event_and_when_it_was_emitted() -> None:
    source = event(30, "u1", "prog-a", 30.0)

    change = json.loads(to_change_json(UPSERT, source))

    emitted_at = change.pop("emitted_at")
    assert emitted_at > source["produced_at"]
    assert change == {
        "op": "upsert",
        "user_id": "u1",
        "programme_id": "prog-a",
        "position_s": 30.0,
        "duration_s": 1800.0,
        "event_time": source["event_time"],
        "event_id": source["event_id"],
        "produced_at": source["produced_at"],
    }


# --- The oracle --------------------------------------------------------------------


def test_oracle_keeps_the_newest_position_per_programme() -> None:
    assert continue_watching_oracle(CASES["in_progress"]) == [
        InProgress("u1", "prog-a", 60.0, 1800.0, at(60)),
        InProgress("u1", "prog-b", 0.0, 1800.0, at(90)),
        InProgress("u2", "prog-a", 60.0, 1800.0, at(120)),
    ]


def test_oracle_leaves_out_programmes_more_than_90_percent_watched() -> None:
    assert continue_watching_oracle(CASES["finished"]) == [
        InProgress("u1", "prog-b", 1620.0, 1800.0, at(30)),
    ]


def test_oracle_ignores_late_and_repeated_events() -> None:
    assert continue_watching_oracle(CASES["late_and_repeated"]) == [
        InProgress("u1", "prog-a", 60.0, 1800.0, at(60)),
        InProgress("u2", "prog-b", 120.0, 1800.0, at(60)),
    ]


def test_oracle_lists_a_programme_watched_again() -> None:
    assert continue_watching_oracle(CASES["watched_again"]) == [
        InProgress("u1", "prog-b", 30.0, 1800.0, at(3_630)),
    ]


def test_oracle_does_not_depend_on_arrival_order() -> None:
    events = CASES["generated"]

    assert continue_watching_oracle(reversed(events)) == continue_watching_oracle(
        events
    )


# --- The job in Flink against the oracle -------------------------------------------


def run_flink(events: list[dict[str, Any]], parallelism: int) -> list[dict[str, Any]]:
    """The job's keyed state and changes on an in-memory source, in order."""
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(parallelism)

    stream = env.from_collection(
        [json.dumps(event) for event in events], type_info=Types.STRING()
    )

    with continue_watching(stream).execute_and_collect() as changes:
        return [json.loads(change) for change in changes]


def applied(changes: list[dict[str, Any]]) -> dict[tuple[str, str], InProgress]:
    """The list a writer would leave after applying the changes in order."""
    listed: dict[tuple[str, str], InProgress] = {}
    for change in changes:
        key = (change["user_id"], change["programme_id"])
        if change["op"] == UPSERT:
            listed[key] = InProgress(
                user_id=change["user_id"],
                programme_id=change["programme_id"],
                position_s=change["position_s"],
                duration_s=change["duration_s"],
                event_time=change["event_time"],
            )
        else:
            del listed[key]
    return listed


@pytest.mark.slow
@pytest.mark.parametrize(
    ("case", "parallelism"),
    [*((case, 1) for case in sorted(CASES)), ("generated", 2)],
)
def test_flink_matches_the_oracle(case: str, parallelism: int) -> None:
    events = CASES[case]

    changes = run_flink(events, parallelism)

    assert applied(changes) == oracle_by_key(events)
    # Each programme's changes come out newest-last, so applying them in
    # order is safe.
    order: defaultdict[tuple[str, str], list[tuple[int, str]]] = defaultdict(list)
    for change in changes:
        key = (change["user_id"], change["programme_id"])
        order[key].append((change["event_time"], change["event_id"]))
    assert all(times == sorted(set(times)) for times in order.values())
