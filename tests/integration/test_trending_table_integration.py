"""Integration tests for the trending table against DynamoDB Local."""

import time
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from testcontainers.core.container import DockerContainer

from realtime_recommendation_pipeline.common.trending_table import (
    create_table_if_missing,
    dynamodb_resource,
    latest_window,
    load_table_definition,
    put_window,
    window_item,
)

pytestmark = pytest.mark.integration

DEFINITION = load_table_definition(
    Path(__file__).resolve().parents[2] / "infra" / "dynamodb" / "trending-table.json"
)


@pytest.fixture(scope="module")
def dynamodb() -> Iterator[Any]:
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

        assert create_table_if_missing(resource.meta.client, DEFINITION) is True
        yield resource


def test_creating_the_table_again_is_a_no_op(dynamodb: Any) -> None:
    assert create_table_if_missing(dynamodb.meta.client, DEFINITION) is False


def test_table_matches_the_definition(dynamodb: Any) -> None:
    client = dynamodb.meta.client

    table = client.describe_table(TableName="trending")["Table"]
    ttl = client.describe_time_to_live(TableName="trending")["TimeToLiveDescription"]

    assert table["KeySchema"] == DEFINITION["KeySchema"]
    assert ttl == {"TimeToLiveStatus": "ENABLED", "AttributeName": "ttl"}


def test_latest_window_reads_back_the_newest(dynamodb: Any) -> None:
    table = dynamodb.Table("trending")
    now = datetime.now(UTC)

    older = window_item(
        "2026-09-21T18:05:00Z", [{"rank": 1, "programme_id": "a", "score": 2}], now
    )
    newer = window_item(
        "2026-09-21T18:10:00Z",
        [
            {"rank": 1, "programme_id": "b", "score": 5},
            {"rank": 2, "programme_id": "a", "score": 3},
        ],
        now,
    )

    put_window(table, newer)
    put_window(table, older)  # written last, but an earlier window

    assert latest_window(table, now) == newer


def test_expired_windows_are_not_served(dynamodb: Any) -> None:
    table = dynamodb.Table("trending")
    now = datetime.now(UTC)

    put_window(table, window_item("2026-09-21T18:15:00Z", [], now))

    assert latest_window(table, now + timedelta(hours=25)) is None
