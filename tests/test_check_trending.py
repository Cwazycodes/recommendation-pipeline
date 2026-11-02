from typing import Any

from realtime_recommendation_pipeline.common.kafka import downstream_consumer_config
from realtime_recommendation_pipeline.tools.check_trending import check_lists


def ranked_list(window_end: str, size: int = 3) -> list[dict[str, Any]]:
    return [{"window_end": window_end, "rank": rank} for rank in range(1, size + 1)]


def test_downstream_consumers_only_read_committed_rows() -> None:
    config = downstream_consumer_config("localhost:9092", group_id="test")

    assert config["isolation.level"] == "read_committed"


def test_lists_every_slide_pass() -> None:
    rows = ranked_list("2026-09-21T12:05:00Z") + ranked_list("2026-09-21T12:10:00Z")

    assert check_lists(rows, slide_minutes=5) == []


def test_a_missing_list_is_a_gap() -> None:
    rows = ranked_list("2026-09-21T12:05:00Z") + ranked_list("2026-09-21T12:15:00Z")

    assert check_lists(rows, slide_minutes=5) == [
        "gap: no list between 2026-09-21T12:05:00Z and 2026-09-21T12:15:00Z"
    ]


def test_a_window_written_twice_is_a_duplicate() -> None:
    rows = ranked_list("2026-09-21T12:05:00Z") * 2

    assert check_lists(rows, slide_minutes=5) == [
        "duplicate: 2026-09-21T12:05:00Z has rank(s) [1, 2, 3] twice"
    ]


def test_missing_ranks_are_reported() -> None:
    rows = [
        {"window_end": "2026-09-21T12:05:00Z", "rank": 1},
        {"window_end": "2026-09-21T12:05:00Z", "rank": 3},
    ]

    assert check_lists(rows, slide_minutes=5) == [
        "broken ranks: 2026-09-21T12:05:00Z has [1, 3]"
    ]


def test_no_rows_is_a_problem() -> None:
    assert check_lists([], slide_minutes=5) == ["no trending rows found"]
