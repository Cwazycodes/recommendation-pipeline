"""Group trending-results rows into one DynamoDB item per window."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from realtime_recommendation_pipeline.common.trending_table import (
    PARTITION_KEY,
    window_item,
)

# A Kafka (topic, partition).
Partition = tuple[str, int]


@dataclass
class Window:
    """The rows read so far for one window, and the offset of its first row."""

    window_end: str
    rows: list[dict[str, Any]] = field(default_factory=list)
    first_offsets: dict[Partition, int] = field(default_factory=dict)


class WindowBuffer:
    """Holds the rows of the window being read until that window is complete.

    Flink emits a window's rows together, in one transaction, and windows one
    after another, so a row from a different window means the buffered window
    has all its rows.
    """

    def __init__(self) -> None:
        self.current: Window | None = None
        self.next_offsets: dict[Partition, int] = {}

    def add(
        self, partition: Partition, offset: int, row: dict[str, Any]
    ) -> Window | None:
        """Buffer a row. Returns the previous window if this row completes it."""
        complete = None
        if self.current is not None and self.current.window_end != row["window_end"]:
            complete, self.current = self.current, None

        if self.current is None:
            self.current = Window(row["window_end"])

        self.current.rows.append(row)
        self.current.first_offsets.setdefault(partition, offset)
        self.next_offsets[partition] = offset + 1

        return complete

    def take(self) -> Window | None:
        """Remove and return the buffered window, if there is one."""
        window, self.current = self.current, None
        return window

    def committable_offsets(self) -> dict[Partition, int]:
        """Offsets that are safe to commit: up to, not into, the buffered window."""
        offsets = dict(self.next_offsets)
        if self.current is not None:
            offsets.update(self.current.first_offsets)

        return offsets

    def clear(self) -> None:
        """Forget everything, e.g. when partitions are revoked."""
        self.current = None
        self.next_offsets.clear()


def emitted_at(window: Window) -> datetime:
    """When Flink emitted the window's list: the latest emitted_at of its rows."""
    return datetime.fromtimestamp(
        max(row["emitted_at"] for row in window.rows) / 1000, UTC
    )


def window_to_item(
    window: Window, partition_key: str = PARTITION_KEY
) -> dict[str, Any]:
    """The table item for a window, identical every time its rows are replayed.

    generated_at is when Flink emitted the list (the latest emitted_at), not
    when the writer ran, so writing a window again writes the same data. The
    writer adds written_at, its own clock, as it writes. If a rank appears
    twice, the later row wins, so an item never repeats a rank.
    """
    by_rank = {row["rank"]: row for row in window.rows}
    ranked = [
        {
            "rank": rank,
            "programme_id": by_rank[rank]["programme_id"],
            "score": by_rank[rank]["score"],
        }
        for rank in sorted(by_rank)
    ]

    return window_item(window.window_end, ranked, emitted_at(window), partition_key)
