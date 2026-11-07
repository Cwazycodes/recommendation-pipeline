"""Create the pipeline's Kafka topics, named by the KAFKA_* settings (make topics)."""

from __future__ import annotations

import argparse
from collections.abc import Sequence

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from realtime_recommendation_pipeline.settings import KafkaSettings, load

# Records carry synthetic event times, some weeks in the past, and Kafka's
# time-based retention reads record timestamps, so the event topics keep
# their records forever on the local stack.
KEEP_FOREVER = {"retention.ms": "-1"}


def pipeline_topics(kafka: KafkaSettings) -> list[NewTopic]:
    """Partitions as the design needs them: the writer reads one partition."""
    return [
        NewTopic(kafka.events_topic, 6, 1, config=KEEP_FOREVER),
        NewTopic(kafka.events_dlq_topic, 6, 1, config=KEEP_FOREVER),
        NewTopic(kafka.trending_topic, 1, 1),
        NewTopic(kafka.writer_dlq_topic, 1, 1),
    ]


def main(argv: Sequence[str] | None = None) -> None:
    argparse.ArgumentParser(description=__doc__).parse_args(argv)

    kafka = load(KafkaSettings)
    admin = AdminClient({"bootstrap.servers": kafka.bootstrap_servers})

    for topic, future in admin.create_topics(pipeline_topics(kafka)).items():
        try:
            future.result(30)
            print(f"Created topic {topic}")
        except KafkaException as exc:
            if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise
            print(f"Topic {topic} already exists")


if __name__ == "__main__":
    main()
