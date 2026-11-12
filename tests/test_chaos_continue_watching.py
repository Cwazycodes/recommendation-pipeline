from typing import Any

from realtime_recommendation_pipeline.common.continue_watching_table import (
    progress_item,
)
from realtime_recommendation_pipeline.common.trending_table import plain
from realtime_recommendation_pipeline.continue_watching_oracle import (
    continue_watching_oracle,
)
from realtime_recommendation_pipeline.producer.generate import generate_events
from realtime_recommendation_pipeline.tools.chaos import (
    compare_listings,
    listed_programmes,
    oracle_programmes,
)

EVENTS = generate_events(1_500, seed=3, dup_rate=0.05)


def as_items(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """What a writer that applied every change would leave, read back from
    DynamoDB: the oracle's programmes as items, with Decimals turned back."""
    return [
        plain(
            progress_item(
                {
                    "user_id": row.user_id,
                    "programme_id": row.programme_id,
                    "position_s": row.position_s,
                    "duration_s": row.duration_s,
                    "event_time": row.event_time,
                    "event_id": "any",
                    "emitted_at": 1_793_000_000_000,
                }
            )
        )
        for row in continue_watching_oracle(events)
    ]


def test_a_table_that_matches_the_oracle_has_no_problems() -> None:
    expected = oracle_programmes(continue_watching_oracle(EVENTS))

    assert expected
    assert compare_listings(listed_programmes(as_items(EVENTS)), expected) == []


def test_missing_extra_and_wrong_programmes_are_reported() -> None:
    expected = oracle_programmes(continue_watching_oracle(EVENTS))
    items = as_items(EVENTS)
    del items[0]
    items[1] = {**items[1], "position_s": items[1]["position_s"] + 30}
    items.append({**items[2], "sk": "programme-9999"})

    problems = compare_listings(listed_programmes(items), expected)

    assert problems[0].startswith("1 in-progress programmes are missing, e.g. user-")
    assert problems[1].startswith("1 programmes are listed but should not be")
    assert problems[2].startswith("1 programmes have the wrong position")


def test_listing_is_keyed_by_user_and_programme() -> None:
    (item,) = as_items(EVENTS)[:1]

    ((key, value),) = listed_programmes([item]).items()

    assert key == (item["pk"].removeprefix("USER#"), item["sk"])
    assert value == (item["position_s"], item["duration_s"], item["last_watched_at"])
