from pyflink.common import Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaSource,
)


def main() -> None:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers("kafka:29092")
        .set_topics("viewing-events")
        .set_group_id("flink-smoke")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    events = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        "viewing-events-source",
    )

    running_count = (
        events.map(lambda _event: 1, output_type=Types.INT())
        .key_by(lambda _value: "all")
        .reduce(lambda left, right: left + right)
    )

    running_count.print()

    env.execute("viewing-events-smoke")


if __name__ == "__main__":
    main()
