"""Plain-Python reference implementation of continue watching, used to check Flink.

Like trending_oracle.py, it imports nothing from the Flink job, so a bug in the
job's logic cannot hide by also being in the oracle. It is also written the
other way round: the job updates its state event by event, while the oracle
looks at all the events at once. For each user and programme it takes the
newest event, by event_time and then event_id, and lists the programme unless
that event is more than 90% of the way through it.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

FINISHED_FRACTION = 0.9


@dataclass(frozen=True, order=True)
class InProgress:
    """One programme on a user's continue-watching list. event_time is epoch ms."""

    user_id: str
    programme_id: str
    position_s: float
    duration_s: float | None
    event_time: int


def continue_watching_oracle(events: Iterable[Mapping[str, Any]]) -> list[InProgress]:
    """Every user's in-progress programmes once all the events have been seen."""
    newest: dict[tuple[str, str], Mapping[str, Any]] = {}
    for event in events:
        key = (event["user_id"], event["programme_id"])
        kept = newest.get(key)
        if kept is None or (event["event_time"], event["event_id"]) > (
            kept["event_time"],
            kept["event_id"],
        ):
            newest[key] = event

    return sorted(
        InProgress(
            user_id=user_id,
            programme_id=programme_id,
            position_s=event["position_s"],
            duration_s=event["duration_s"],
            event_time=event["event_time"],
        )
        for (user_id, programme_id), event in newest.items()
        if not (
            event["duration_s"]
            and event["position_s"] > FINISHED_FRACTION * event["duration_s"]
        )
    )
