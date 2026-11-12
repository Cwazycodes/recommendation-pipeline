"""Continue watching: each user's in-progress programmes, as upserts and deletes."""

import argparse

from pyflink.common import Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource

from realtime_recommendation_pipeline.flink_jobs.continue_watching import (
    ValidEvents,
    continue_watching,
)
from realtime_recommendation_pipeline.flink_jobs.kafka_sink import build_kafka_sink
from realtime_recommendation_pipeline.flink_jobs.reliability import (
    job_settings,
    to_flink_configuration,
)
from realtime_recommendation_pipeline.settings import (
    ContinueWatchingSettings,
    FlinkSettings,
    KafkaSettings,
    load,
)


def main() -> None:
    # FLINK_*, KAFKA_* and CONTINUE_WATCHING_* come from .env, which Compose
    # passes to this container.
    kafka = load(KafkaSettings)
    flink = load(FlinkSettings)
    settings = load(ContinueWatchingSettings)

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-topic", default=kafka.events_topic)
    parser.add_argument("--continue-watching-topic", default=settings.topic)
    parser.add_argument("--parallelism", type=int, default=1)
    args = parser.parse_args()
    if args.parallelism < 1:
        parser.error("--parallelism must be at least 1")

    env = StreamExecutionEnvironment.get_execution_environment(
        to_flink_configuration(job_settings(flink.checkpoint_dir))
    )
    # The stages that handle each event: the Kafka source, validation and the
    # keyed continue-watching state.
    env.set_parallelism(args.parallelism)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(flink.kafka_bootstrap_servers)
        .set_topics(args.source_topic)
        .set_group_id(settings.source_group_id)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # No windows, so no watermarks: event_time only decides which of two events
    # for the same programme is newer. Named apart from the trending job's
    # source, because both read the events topic (tools/metrics_report.py).
    events = env.from_source(
        source, WatermarkStrategy.no_watermarks(), "continue-watching-source"
    ).uid("continue-watching-source")

    valid_events = events.flat_map(ValidEvents(), output_type=Types.STRING()).name(
        "valid-events"
    )

    # One sink subtask writing to a one-partition topic: each user's changes
    # come from one keyed subtask, so they reach the writer in the order they
    # were made, in transactions that commit with each checkpoint.
    (
        continue_watching(valid_events)
        .sink_to(
            build_kafka_sink(
                flink.kafka_bootstrap_servers,
                args.continue_watching_topic,
                exactly_once=True,
            )
        )
        .name("continue-watching-sink")
        .uid("continue-watching-sink")
        .set_parallelism(1)
    )

    env.execute("continue-watching-job")


if __name__ == "__main__":
    main()
