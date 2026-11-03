"""Shared setup for the Docker integration tests."""

import time
from collections.abc import Iterator

import pytest
from testcontainers.core.docker_client import DockerClient


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
