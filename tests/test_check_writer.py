from typing import Any

from realtime_recommendation_pipeline.tools.check_writer import compare

NOW_S = 1_790_000_000


def item(score: int, **extra: Any) -> dict[str, Any]:
    return {
        "pk": "TRENDING#1h",
        "sk": "2026-09-21T14:00:00Z",
        "items": [{"rank": 1, "programme_id": "programme-0001", "score": score}],
        "generated_at": "2026-09-21T14:00:31Z",
        "ttl": NOW_S + 3600,
        **extra,
    }


def test_written_at_is_not_compared() -> None:
    # The writer stamps its own clock; rewriting a window changes only that.
    expected = {"2026-09-21T14:00:00Z": item(9)}
    actual = {"2026-09-21T14:00:00Z": item(9, written_at="2026-09-21T14:00:38.500Z")}

    assert compare(expected, actual, NOW_S) == []


def test_a_wrong_ranked_list_is_still_found() -> None:
    expected = {"2026-09-21T14:00:00Z": item(9)}
    actual = {"2026-09-21T14:00:00Z": item(8, written_at="2026-09-21T14:00:38.500Z")}

    assert compare(expected, actual, NOW_S) == [
        "wrong: 2026-09-21T14:00:00Z differs from the topic"
    ]
