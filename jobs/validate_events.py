"""Validate viewing events and dead-letter the ones that break the contract."""

import json

from pyflink.common import Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import (
    OutputTag,
    ProcessFunction,
    RuntimeContext,
    StreamExecutionEnvironment,
)
from pyflink.datastream.connectors.base import DeliveryGuarantee
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.table import StreamTableEnvironment

from realtime_recommendation_pipeline.flink_jobs.dedup import (
    DeduplicateEvents,
    event_id_of,
)
from realtime_recommendation_pipeline.flink_jobs.event_time import (
    to_events_table,
    viewing_event_watermarks,
)
from realtime_recommendation_pipeline.flink_jobs.validation import validate_event

BOOTSTRAP_SERVERS = "kafka:29092"
SOURCE_TOPIC = "viewing-events"
DLQ_TOPIC = "viewing-events-dlq"

# One shared tag: the function emits to it and get_side_output() reads it.
DLQ_TAG = OutputTag("dead-letters", Types.STRING())


class ValidateEvents(ProcessFunction):
    """Send valid events downstream and invalid ones to the DLQ side output."""

    def open(self, runtime_context: RuntimeContext) -> None:
        metrics = runtime_context.get_metrics_group()
        self.valid_events = metrics.counter("valid_events")
        self.dead_lettered_events = metrics.counter("dead_lettered_events")

    def process_element(self, value, ctx):
        result = validate_event(value)

        if result.event is not None:
            self.valid_events.inc()
            yield result.event.model_dump_json()
            return

        self.dead_lettered_events.inc()
        # PyFlink has no ctx.output(); yield (tag, value) to emit a side output.
        yield DLQ_TAG, json.dumps(result.dead_letter)


def build_dlq_sink() -> KafkaSink:
    return (
        KafkaSink.builder()
        .set_bootstrap_servers(BOOTSTRAP_SERVERS)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(DLQ_TOPIC)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )


def main() -> None:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.enable_checkpointing(10_000)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP_SERVERS)
        .set_topics(SOURCE_TOPIC)
        .set_group_id("flink-validate")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # Watermarks are generated per Kafka partition from the record timestamp,
    # which the producer sets to event_time. PyFlink ignores a Python
    # TimestampAssigner passed to from_source, so the Kafka timestamp is the
    # way to get per-partition event-time watermarks.
    raw_events = env.from_source(
        source,
        viewing_event_watermarks(),
        "viewing-events-source",
    )

    valid_events = raw_events.process(
        ValidateEvents(),
        output_type=Types.STRING(),
    ).name("validate-events")

    dead_letters = valid_events.get_side_output(DLQ_TAG)
    dead_letters.sink_to(build_dlq_sink()).name("viewing-events-dlq-sink")

    unique_events = (
        valid_events.key_by(event_id_of, key_type=Types.STRING())
        .process(DeduplicateEvents(), output_type=Types.STRING())
        .name("deduplicate-events")
    )

    t_env = StreamTableEnvironment.create(env)
    t_env.get_config().set_local_timezone("UTC")

    events_table = to_events_table(t_env, unique_events)
    events_table.print_schema()

    t_env.to_data_stream(events_table).print()

    env.execute("validate-viewing-events")


if __name__ == "__main__":
    main()
