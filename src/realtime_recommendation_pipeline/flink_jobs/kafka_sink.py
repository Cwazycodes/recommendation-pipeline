"""Kafka sinks for the streaming jobs: at-least-once, or exactly-once."""

from __future__ import annotations

from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.base import DeliveryGuarantee
from pyflink.datastream.connectors.kafka import (
    KafkaRecordSerializationSchema,
    KafkaSink,
)

from realtime_recommendation_pipeline.flink_jobs.reliability import (
    TRANSACTION_TIMEOUT_MS,
)


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
