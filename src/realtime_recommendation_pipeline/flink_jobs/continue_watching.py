"""Continue watching: each user's last position in the programmes they have not finished.

Events are keyed by user_id, and for each of the user's programmes the job
keeps the newest event it has seen: newest by event_time, then by event_id.
An event no newer than that one changes nothing, so a repeat or an older event
that arrives late cannot move a position backwards, and the result does not
depend on the order events arrive in. More than 90% watched counts as
finished: the programme leaves the list, and the job remembers that it did,
so an older event cannot bring it back. Each change is published as an upsert
or a delete (docs/design.md, "Continue watching").
"""

from __future__ import annotations

import json
import time
from collections.abc import Iterator, Mapping
from dataclasses import asdict, dataclass
from typing import Any

from pyflink.common import Types
from pyflink.common.time import Time
from pyflink.datastream import (
    DataStream,
    FlatMapFunction,
    KeyedProcessFunction,
    RuntimeContext,
)
from pyflink.datastream.state import MapStateDescriptor, StateTtlConfig

from realtime_recommendation_pipeline.flink_jobs.validation import validate_event

# More than this share of a programme's duration watched counts as finished.
FINISHED_FRACTION = 0.9

# A programme with no new event for 30 days leaves the job's state, and its item
# leaves the table at the same age (common/continue_watching_table.py).
PROGRESS_TTL = Time.days(30)

UPSERT = "upsert"
DELETE = "delete"


@dataclass(frozen=True)
class Progress:
    """The newest event seen for one user and programme, and whether it finished it."""

    event_time: int
    event_id: str
    position_s: float
    duration_s: float | None
    finished: bool


def is_finished(position_s: float, duration_s: float | None) -> bool:
    """More than 90% watched. Without a duration, nothing counts as finished."""
    if not duration_s:
        return False
    return position_s > FINISHED_FRACTION * duration_s


def advance(
    previous: Progress | None, event: Mapping[str, Any]
) -> tuple[Progress, str | None]:
    """The programme's progress after this event, and the change to publish.

    - An event no newer than the one kept changes nothing.
    - A newer event becomes the one kept. If it does not finish the programme,
      it is published as an upsert.
    - The event that finishes a programme in the list is published as a delete.
      Its progress is kept, marked finished, so a later, unfinished event
      (watching it again) brings the programme back but an older one cannot.
    """
    newest = (event["event_time"], event["event_id"])
    if previous is not None and newest <= (previous.event_time, previous.event_id):
        return previous, None

    progress = Progress(
        event_time=event["event_time"],
        event_id=event["event_id"],
        position_s=event["position_s"],
        duration_s=event["duration_s"],
        finished=is_finished(event["position_s"], event["duration_s"]),
    )
    if not progress.finished:
        return progress, UPSERT
    if previous is not None and not previous.finished:
        return progress, DELETE
    # Finished before it was ever listed, or already finished: nothing to remove.
    return progress, None


def to_change_json(change: str, event: Mapping[str, Any]) -> str:
    """Kafka message for one change, stamped with emitted_at."""
    return json.dumps(
        {
            "op": change,
            "user_id": event["user_id"],
            "programme_id": event["programme_id"],
            "position_s": event["position_s"],
            "duration_s": event["duration_s"],
            "event_time": event["event_time"],
            "event_id": event["event_id"],
            "produced_at": event["produced_at"],
            "emitted_at": time.time_ns() // 1_000_000,
        }
    )


def progress_ttl_config() -> StateTtlConfig:
    """Expire each programme's progress 30 days after its last update."""
    return (
        StateTtlConfig.new_builder(PROGRESS_TTL)
        .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite)
        .set_state_visibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
        .build()
    )


class ValidEvents(FlatMapFunction):
    """Pass on valid events and drop the rest.

    The trending job dead-letters invalid events, so this job only counts them.
    """

    def open(self, runtime_context: RuntimeContext) -> None:
        metrics = runtime_context.get_metrics_group()
        self.valid_events = metrics.counter("valid_events")
        self.invalid_events = metrics.counter("invalid_events")

    def flat_map(self, value: str) -> Iterator[str]:
        result = validate_event(value)
        if result.event is None:
            self.invalid_events.inc()
            return

        self.valid_events.inc()
        yield result.event.model_dump_json()


def user_id_of(event_json: str) -> str:
    """Key selector: the user_id of a validated event."""
    return str(json.loads(event_json)["user_id"])


class ContinueWatching(KeyedProcessFunction):
    """Keeps each user's progress per programme and emits the changes."""

    def open(self, runtime_context: RuntimeContext) -> None:
        # programme_id -> the Progress kept for it, as JSON.
        descriptor = MapStateDescriptor("progress", Types.STRING(), Types.STRING())
        descriptor.enable_time_to_live(progress_ttl_config())
        self.progress = runtime_context.get_map_state(descriptor)

        metrics = runtime_context.get_metrics_group()
        self.upserts = metrics.counter("continue_watching_upserts")
        self.deletes = metrics.counter("continue_watching_deletes")
        self.ignored_events = metrics.counter("continue_watching_ignored_events")

    def process_element(self, value: str, ctx: Any) -> Iterator[str]:
        event = json.loads(value)
        programme_id = event["programme_id"]

        stored = self.progress.get(programme_id)
        previous = None if stored is None else Progress(**json.loads(stored))
        progress, change = advance(previous, event)

        if previous is not None and progress is previous:
            self.ignored_events.inc()
            return

        self.progress.put(programme_id, json.dumps(asdict(progress)))
        if change == UPSERT:
            self.upserts.inc()
        elif change == DELETE:
            self.deletes.inc()
        if change is not None:
            yield to_change_json(change, event)


def continue_watching(events: DataStream) -> DataStream:
    """Key validated events by user_id; the changes, as JSON."""
    return (
        events.key_by(user_id_of, key_type=Types.STRING())
        .process(ContinueWatching(), output_type=Types.STRING())
        .name("track-progress")
        .uid("track-progress")
    )
