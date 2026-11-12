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
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
from pyflink.table import StreamTableEnvironment

from realtime_recommendation_pipeline.common.event_schemas import WriterSchemas
from realtime_recommendation_pipeline.common.schema_registry import SchemaRegistry
from realtime_recommendation_pipeline.flink_jobs.dedup import deduplicate
from realtime_recommendation_pipeline.flink_jobs.event_time import (
    to_events_table,
    viewing_event_watermarks,
)
from realtime_recommendation_pipeline.flink_jobs.job_config import parse_job_config
from realtime_recommendation_pipeline.flink_jobs.kafka_sink import build_kafka_sink
from realtime_recommendation_pipeline.flink_jobs.reliability import (
    job_settings,
    to_flink_configuration,
)
from realtime_recommendation_pipeline.flink_jobs.trending import (
    to_trending_json,
    trending_table,
)
from realtime_recommendation_pipeline.flink_jobs.validation import (
    KAFKA_VALUE_CHARSET,
    validate_event,
)
from realtime_recommendation_pipeline.settings import FlinkSettings, KafkaSettings, load

# One shared tag: the function emits to it and get_side_output() reads it.
DLQ_TAG = OutputTag("dead-letters", Types.STRING())


class ValidateEvents(ProcessFunction):
    """Send valid events downstream and invalid ones to the DLQ side output."""

    def __init__(self, writer_schemas: WriterSchemas) -> None:
        self.writer_schemas = writer_schemas

    def open(self, runtime_context: RuntimeContext) -> None:
        metrics = runtime_context.get_metrics_group()
        self.valid_events = metrics.counter("valid_events")
        self.dead_lettered_events = metrics.counter("dead_lettered_events")

    def process_element(self, value, ctx):
        result = validate_event(value.encode(KAFKA_VALUE_CHARSET), self.writer_schemas)

        if result.event is not None:
            self.valid_events.inc()
            yield result.event.model_dump_json()
            return

        self.dead_lettered_events.inc()
        # PyFlink has no ctx.output(); yield (tag, value) to emit a side output.
        yield DLQ_TAG, json.dumps(result.dead_letter)


def main() -> None:
    # FLINK_* and KAFKA_* come from .env, which Compose passes to this container.
    kafka = load(KafkaSettings)
    flink = load(FlinkSettings)
    job = parse_job_config(kafka)
    env = StreamExecutionEnvironment.get_execution_environment(
        to_flink_configuration(job_settings(flink.checkpoint_dir))
    )
    # The stages that handle each event: the Kafka source, validation and
    # deduplication (make bench PARALLELISM=2; see docs/benchmarks.md).
    env.set_parallelism(job.parallelism)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(flink.kafka_bootstrap_servers)
        .set_topics(job.source_topic)
        .set_group_id(flink.source_group_id)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        # Avro bytes, as a string that encodes back to them (validation.py).
        .set_value_only_deserializer(SimpleStringSchema(KAFKA_VALUE_CHARSET))
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

    registry = SchemaRegistry(flink.schema_registry_url)

    valid_events = raw_events.process(
        ValidateEvents(WriterSchemas(registry.schema)),
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
    # Windowing, ranking and the trending sink run as one whatever the
    # parallelism, so each window's rows reach the topic together, in one
    # transaction, as the results writer expects (docs/design.md).
    t_env.get_config().set("table.exec.resource.default-parallelism", "1")

    events_table = to_events_table(t_env, unique_events)
    events_table.print_schema()

    trending = trending_table(t_env, events_table, job.trending)

    (
        t_env.to_data_stream(trending)
        .map(to_trending_json, output_type=Types.STRING())
        .name("to-trending-json")
        .set_parallelism(1)
        .sink_to(
            build_kafka_sink(
                flink.kafka_bootstrap_servers, job.trending_topic, exactly_once=True
            )
        )
        .name("trending-results-sink")
        .uid("trending-results-sink")
        .set_parallelism(1)
    )

    env.execute("validate-viewing-events")


if __name__ == "__main__":
    main()
