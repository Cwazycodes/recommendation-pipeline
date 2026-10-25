"""Plain-Python reference implementation of trending, used to check Flink.

It deliberately imports nothing from the Flink job, so a bug in the job's
logic cannot hide by also being in the oracle.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

SCORED_EVENT_TYPES = frozenset({"play_start", "heartbeat"})

WindowKey = tuple[int, int, str]


@dataclass(frozen=True, order=True)
class TrendingRow:
    """One ranked programme in one window. Times are epoch milliseconds."""

    window_start: int
    window_end: int
    rank: int
    programme_id: str
    score: int
    max_produced_at: int


def windows_containing(
    event_time: int, size_ms: int, slide_ms: int
) -> list[tuple[int, int]]:
    """Every sliding window [start, end) that contains event_time."""
    last_start = event_time - event_time % slide_ms

    return [
        (start, start + size_ms)
        for start in range(last_start, event_time - size_ms, -slide_ms)
    ]


def trending_oracle(
    events: Iterable[Mapping[str, Any]],
    *,
    window_size_ms: int,
    window_slide_ms: int,
    top_n: int,
    max_out_of_orderness_ms: int,
) -> list[TrendingRow]:
    """Trending rows the job should emit for events in arrival order.

    - Repeat event_ids are dropped; the first copy wins.
    - The watermark is the newest event_time so far minus the bound, and it
      moves after each event.
    - A window fires once the watermark reaches window_end - 1 ms. An event
      only counts in windows that have not fired when it arrives.
    - At the end of the input every remaining window fires.
    """
    seen_event_ids: set[str] = set()
    viewers: defaultdict[WindowKey, set[str]] = defaultdict(set)
    max_produced_at: dict[WindowKey, int] = {}
    newest_event_time: int | None = None
    watermark: int | None = None

    for event in events:
        if event["event_id"] in seen_event_ids:
            continue
        seen_event_ids.add(event["event_id"])

        event_time = event["event_time"]

        if event["event_type"] in SCORED_EVENT_TYPES:
            for start, end in windows_containing(
                event_time, window_size_ms, window_slide_ms
            ):
                if watermark is not None and end - 1 <= watermark:
                    continue  # this window already fired

                key = (start, end, event["programme_id"])
                viewers[key].add(event["user_id"])
                max_produced_at[key] = max(
                    max_produced_at.get(key, event["produced_at"]),
                    event["produced_at"],
                )

        if newest_event_time is None or event_time > newest_event_time:
            newest_event_time = event_time
        watermark = newest_event_time - max_out_of_orderness_ms

    scores_by_window: defaultdict[tuple[int, int], list[tuple[str, int]]] = defaultdict(
        list
    )
    for (start, end, programme_id), users in viewers.items():
        scores_by_window[(start, end)].append((programme_id, len(users)))

    rows = []
    for (start, end), scores in scores_by_window.items():
        ranked = sorted(scores, key=lambda item: (-item[1], item[0]))[:top_n]

        for rank, (programme_id, score) in enumerate(ranked, start=1):
            rows.append(
                TrendingRow(
                    window_start=start,
                    window_end=end,
                    rank=rank,
                    programme_id=programme_id,
                    score=score,
                    max_produced_at=max_produced_at[(start, end, programme_id)],
                )
            )

    return sorted(rows)
