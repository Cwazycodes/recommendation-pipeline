"""Kafka settings shared by consumers of the pipeline's output."""

from __future__ import annotations


def downstream_consumer_config(bootstrap_servers: str, group_id: str) -> dict[str, str]:
    """Consumer settings for reading the Flink job's exactly-once output.

    read_committed hides rows from transactions Flink aborted after a failure,
    and holds rows back until the checkpoint that wrote them has committed.
    """
    return {
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": "false",
        "isolation.level": "read_committed",
    }
