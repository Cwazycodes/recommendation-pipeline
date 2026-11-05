from datetime import datetime
from itertools import pairwise
from pathlib import Path
from typing import Any

import pytest

from realtime_recommendation_pipeline.producer.generate import generate_events
from realtime_recommendation_pipeline.tools.chaos import (
    END_MARKER,
    START_MARKER,
    DrillReport,
    DrillResult,
    compare_with_oracle,
    first_few,
    iso,
    record_results,
    render_results,
    synthetic_windows,
)
from realtime_recommendation_pipeline.trending_oracle import (
    TrendingRow,
    trending_oracle,
)


def as_message(row: TrendingRow) -> dict[str, Any]:
    """An oracle row as the job writes it to trending-results."""
    return {
        "window_start": iso(row.window_start),
        "window_end": iso(row.window_end),
        "rank": row.rank,
        "programme_id": row.programme_id,
        "score": row.score,
        "max_produced_at": row.max_produced_at,
        "emitted_at": 0,
    }


@pytest.fixture(scope="module")
def expected() -> list[TrendingRow]:
    return trending_oracle(
        generate_events(800, seed=1, dup_rate=0.05),
        window_size_ms=3_600_000,
        window_slide_ms=300_000,
        top_n=20,
        max_out_of_orderness_ms=30_000,
    )


def test_rows_that_match_the_oracle_have_no_problems(
    expected: list[TrendingRow],
) -> None:
    assert compare_with_oracle([as_message(row) for row in expected], expected) == []


def test_windows_after_the_jobs_last_one_are_not_compared(
    expected: list[TrendingRow],
) -> None:
    # The job's watermark had not passed the last windows yet.
    last_end = max(row.window_end for row in expected)
    committed = [as_message(row) for row in expected if row.window_end < last_end]

    assert compare_with_oracle(committed, expected) == []


def test_missing_repeated_and_wrong_rows_are_reported(
    expected: list[TrendingRow],
) -> None:
    rows = [as_message(row) for row in expected]
    middle = len(rows) // 2
    repeated = rows[middle]
    wrong = {**rows[middle + 1], "score": rows[middle + 1]["score"] + 100}
    del rows[middle + 1]
    rows += [repeated, wrong]
    del rows[1]

    problems = compare_with_oracle(rows, expected)

    assert problems[0] == "1 rows were committed more than once"
    assert problems[1].startswith("2 oracle rows are missing")
    assert problems[2].startswith("1 rows are not in the oracle")


def test_no_rows_at_all_is_a_problem(expected: list[TrendingRow]) -> None:
    assert compare_with_oracle([], expected) == ["no trending rows were committed"]


def test_synthetic_windows_look_like_the_jobs_output() -> None:
    windows = synthetic_windows(3, emitted_at_ms=1_793_000_000_000)

    ends = [datetime.fromisoformat(rows[0]["window_end"]) for rows in windows]
    gaps = [(later - earlier).total_seconds() for earlier, later in pairwise(ends)]
    assert gaps == [300, 300]
    for rows in windows:
        assert [row["rank"] for row in rows] == list(range(1, 21))
        assert len({row["programme_id"] for row in rows}) == 20
        scores = [row["score"] for row in rows]
        assert scores == sorted(scores, reverse=True)
        assert {row["emitted_at"] for row in rows} == {1_793_000_000_000}


def test_recording_replaces_only_the_results_block(tmp_path: Path) -> None:
    doc = tmp_path / "reliability.md"
    doc.write_text(
        f"# Drills\n\nHand-written.\n\n{START_MARKER}\nold\n{END_MARKER}\n\nEnd.\n"
    )
    results = [
        DrillResult("writer", "Writer killed", DrillReport(observed=["fine"]), 3.0),
        DrillResult(
            "kafka", "Kafka restarted", DrillReport(problems=["lost a | event"]), 9.0
        ),
    ]

    record_results(doc, render_results(results, "on a test run"))

    text = doc.read_text()
    assert text.startswith("# Drills\n\nHand-written.\n\n" + START_MARKER)
    assert text.endswith(END_MARKER + "\n\nEnd.\n")
    assert "old" not in text
    assert "| Writer killed | Pass | fine |" in text
    assert "| Kafka restarted | **Fail** | **lost a / event** |" in text
    assert "Recorded with `make chaos` on a test run." in text


def test_a_long_list_of_problems_is_cut_short() -> None:
    assert first_few(["a", "b"]) == ["a", "b"]
    assert first_few(["a", "b", "c", "d", "e"]) == ["a", "b", "c", "and 2 more"]
