"""Shared setup for the Docker integration tests."""

import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.docker_client import DockerClient

from realtime_recommendation_pipeline.common.trending_table import (
    create_table_if_missing,
    dynamodb_resource,
    load_table_definition,
)

TABLE_DEFINITION = load_table_definition(
    Path(__file__).resolve().parents[2] / "infra" / "dynamodb" / "trending-table.json"
)


@pytest.fixture(scope="session", autouse=True)
def retry_port_lookups() -> Iterator[None]:
    """Give Docker a moment to publish a container's ports.

    Docker Desktop can report a container as running a moment before its
    ports are published, and testcontainers reads the port mapping only once,
    so a lookup straight after start fails with "Port mapping ... is not
    available". That hits Ryuk, KafkaContainer and our own containers alike.
    """
    original = DockerClient.port

    def port_with_retry(self: DockerClient, container_id: str, port: int) -> str:
        deadline = time.monotonic() + 5
        while True:
            try:
                return original(self, container_id, port)
            except ConnectionError:
                if time.monotonic() > deadline:
                    raise
                time.sleep(0.1)

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(DockerClient, "port", port_with_retry)
        yield


@pytest.fixture(scope="module")
def dynamodb() -> Iterator[Any]:
    """DynamoDB Local in a throwaway container, with the trending table created."""
    container = (
        DockerContainer("amazon/dynamodb-local:3.3.2")
        .with_command("-jar DynamoDBLocal.jar -inMemory -sharedDb")
        .with_exposed_ports(8000)
    )

    with container:
        host = container.get_container_host_ip()
        resource = dynamodb_resource(
            f"http://{host}:{container.get_exposed_port(8000)}"
        )

        deadline = time.monotonic() + 30
        while True:
            try:
                resource.meta.client.list_tables()
                break
            except Exception:
                if time.monotonic() > deadline:
                    raise
                time.sleep(0.5)

        assert create_table_if_missing(resource.meta.client, TABLE_DEFINITION) is True
        yield resource
