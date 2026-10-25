"""Event-time watermarks and the rowtime table for viewing events."""

from __future__ import annotations

import json

from pyflink.common import Duration, Row, Types, WatermarkStrategy
from pyflink.datastream import DataStream
from pyflink.table import DataTypes, Schema, StreamTableEnvironment, Table

# How far behind the newest event in its Kafka partition an event may arrive
# and still count as on time. See docs/design.md "Event time and lateness".
MAX_OUT_OF_ORDERNESS_S = 30

# A partition that sends nothing for this long stops holding the watermark back.
IDLE_TIMEOUT_S = 60

# Every contract field except event_time, which becomes the rowtime column.
EVENT_ROW_TYPE = Types.ROW_NAMED(
    [
        "event_id",
        "user_id",
        "programme_id",
        "event_type",
        "position_s",
        "duration_s",
        "device",
        "produced_at",
    ],
    [
        Types.STRING(),
        Types.STRING(),
        Types.STRING(),
        Types.STRING(),
        Types.DOUBLE(),
        Types.DOUBLE(),
        Types.STRING(),
        Types.LONG(),
    ],
)


def viewing_event_watermarks() -> WatermarkStrategy:
    """Bounded out-of-orderness on each record's timestamp, with idleness."""
    return WatermarkStrategy.for_bounded_out_of_orderness(
        Duration.of_seconds(MAX_OUT_OF_ORDERNESS_S)
    ).with_idleness(Duration.of_seconds(IDLE_TIMEOUT_S))


def to_event_row(event_json: str) -> Row:
    """Turn a validated event's JSON into a Row matching EVENT_ROW_TYPE."""
    event = json.loads(event_json)

    return Row(
        event["event_id"],
        event["user_id"],
        event["programme_id"],
        event["event_type"],
        event["position_s"],
        event["duration_s"],
        event["device"],
        event["produced_at"],
    )


def to_events_table(t_env: StreamTableEnvironment, events: DataStream) -> Table:
    """Table of events whose event_time column is the stream's rowtime."""
    rows = events.map(to_event_row, output_type=EVENT_ROW_TYPE).name("to-event-rows")

    schema = (
        Schema.new_builder()
        .column_by_metadata("event_time", DataTypes.TIMESTAMP_LTZ(3), "rowtime")
        .watermark("event_time", "SOURCE_WATERMARK()")
        .build()
    )

    return t_env.from_data_stream(rows, schema)
