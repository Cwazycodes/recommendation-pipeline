"""Drop repeat deliveries of the same viewing event."""

from __future__ import annotations

import json
from collections.abc import Iterator
from typing import Any

from pyflink.common import Types
from pyflink.common.time import Time
from pyflink.datastream import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import StateTtlConfig, ValueStateDescriptor

# Long enough to cover redelivery, short enough to bound state.
# The trade-off is written up in docs/design.md under "Deduplication".
SEEN_TTL = Time.hours(1)


def event_id_of(event_json: str) -> str:
    """Key selector: the event_id of a validated event."""
    return str(json.loads(event_json)["event_id"])


def seen_ttl_config() -> StateTtlConfig:
    """Expire each 'seen' flag one TTL after the event was first seen."""
    return (
        StateTtlConfig.new_builder(SEEN_TTL)
        .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite)
        .set_state_visibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
        .build()
    )


class DeduplicateEvents(KeyedProcessFunction):
    """Emit the first copy of each event_id and drop later copies."""

    def open(self, runtime_context: RuntimeContext) -> None:
        descriptor = ValueStateDescriptor("seen", Types.BOOLEAN())
        descriptor.enable_time_to_live(seen_ttl_config())
        self.seen = runtime_context.get_state(descriptor)

        metrics = runtime_context.get_metrics_group()
        self.unique_events = metrics.counter("unique_events")
        self.duplicate_events = metrics.counter("duplicate_events")

    def process_element(self, value: str, ctx: Any) -> Iterator[str]:
        if self.seen.value():
            self.duplicate_events.inc()
            return

        self.seen.update(True)
        self.unique_events.inc()
        yield value
