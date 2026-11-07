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

from realtime_recommendation_pipeline.flink_jobs.dedup import deduplicate
from realtime_recommendation_pipeline.flink_jobs.event_time import (
    to_events_table,
    viewing_event_watermarks,
)
from realtime_recommendation_pipeline.flink_jobs.job_config import parse_job_config
from realtime_recommendation_pipeline.flink_jobs.reliability import (
    TRANSACTION_TIMEOUT_MS,
    job_settings,
    to_flink_configuration,
)
from realtime_recommendation_pipeline.flink_jobs.trending import (
    to_trending_json,
    trending_table,
)
from realtime_recommendation_pipeline.flink_jobs.validation import validate_event
from realtime_recommendation_pipeline.settings import FlinkSettings, KafkaSettings, load

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


def build_kafka_sink(
    bootstrap_servers: str, topic: str, *, exactly_once: bool = False
) -> KafkaSink:
    builder = (
        KafkaSink.builder()
        .set_bootstrap_servers(bootstrap_servers)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
    )

    if not exactly_once:
        return builder.set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE).build()

    # Rows go out in a Kafka transaction that commits when the checkpoint completes. The prefix must be unique per sink and stay the same across restarts, so Flink can find and finish or abort its old transactions.
    return (
        builder.set_delivery_guarantee(DeliveryGuarantee.EXACTLY_ONCE)
        .set_transactional_id_prefix(f"recommendation-pipeline-{topic}")
        .set_property("transaction.timeout.ms", str(TRANSACTION_TIMEOUT_MS))
        .build()
    )


def main() -> None:
    # FLINK_* and KAFKA_* come from .env, which Compose passes to this container.
    kafka = load(KafkaSettings)
    flink = load(FlinkSettings)
    job = parse_job_config(kafka)
    env = StreamExecutionEnvironment.get_execution_environment(
        to_flink_configuration(job_settings(flink.checkpoint_dir))
    )
    env.set_parallelism(1)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(flink.kafka_bootstrap_servers)
        .set_topics(job.source_topic)
        .set_group_id(flink.source_group_id)
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
    ).uid("viewing-events-source")

    valid_events = raw_events.process(
        ValidateEvents(),
        output_type=Types.STRING(),
    ).name("validate-events")

    dead_letters = valid_events.get_side_output(DLQ_TAG)
    (
        dead_letters.sink_to(
            build_kafka_sink(flink.kafka_bootstrap_servers, job.dlq_topic)
        )
        .name("viewing-events-dlq-sink")
        .uid("viewing-events-dlq-sink")
    )

    unique_events = deduplicate(valid_events)

    t_env = StreamTableEnvironment.create(env)
    t_env.get_config().set_local_timezone("UTC")

    events_table = to_events_table(t_env, unique_events)
    events_table.print_schema()

    trending = trending_table(t_env, events_table, job.trending)

    (
        t_env.to_data_stream(trending)
        .map(to_trending_json, output_type=Types.STRING())
        .name("to-trending-json")
        .sink_to(
            build_kafka_sink(
                flink.kafka_bootstrap_servers, job.trending_topic, exactly_once=True
            )
        )
        .name("trending-results-sink")
        .uid("trending-results-sink")
    )

    env.execute("validate-viewing-events")


if __name__ == "__main__":
    main()
