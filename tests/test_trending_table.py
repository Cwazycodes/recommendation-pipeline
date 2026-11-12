from collections.abc import Iterator
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import boto3
import pytest
from moto import mock_aws

from realtime_recommendation_pipeline.common.trending_table import (
    latest_window,
    load_table_definition,
    plain,
    recreate_table,
    window_item,
)

DEFINITION = (
    Path(__file__).resolve().parents[1] / "infra" / "dynamodb" / "trending-table.json"
)
GENERATED_AT = datetime(2026, 11, 2, 12, 0, tzinfo=UTC)
RANKED = [{"rank": 1, "programme_id": "programme-0165", "score": 27}]


class FakeTable:
    """Records the query the code would send to DynamoDB."""

    def __init__(self, items: list[dict[str, Any]]) -> None:
        self.items = items
        self.query_args: dict[str, Any] = {}

    def query(self, **kwargs: Any) -> dict[str, Any]:
        self.query_args = kwargs
        return {"Items": self.items}


def test_table_definition_matches_the_access_pattern() -> None:
    definition = load_table_definition(DEFINITION)

    assert definition["TableName"] == "trending"
    assert definition["BillingMode"] == "PAY_PER_REQUEST"
    assert definition["KeySchema"] == [
        {"AttributeName": "pk", "KeyType": "HASH"},
        {"AttributeName": "sk", "KeyType": "RANGE"},
    ]
    assert definition["AttributeDefinitions"] == [
        {"AttributeName": "pk", "AttributeType": "S"},
        {"AttributeName": "sk", "AttributeType": "S"},
    ]
    assert definition["TimeToLiveSpecification"] == {
        "AttributeName": "ttl",
        "Enabled": True,
    }


def test_window_item_has_the_ticket_shape() -> None:
    item = window_item("2026-09-21T14:15:00Z", RANKED, GENERATED_AT)

    assert item == {
        "pk": "TRENDING#1h",
        "sk": "2026-09-21T14:15:00Z",
        "items": RANKED,
        "generated_at": "2026-11-02T12:00:00Z",
        "ttl": int(GENERATED_AT.timestamp()) + 24 * 60 * 60,
    }


def test_iso_window_ends_sort_in_time_order() -> None:
    window_ends = [
        "2026-09-21T09:55:00Z",
        "2026-09-21T10:00:00Z",
        "2026-09-22T00:00:00Z",
    ]

    assert sorted(window_ends) == window_ends


def test_latest_window_is_one_backwards_query_for_one_item() -> None:
    table = FakeTable(items=[])

    latest_window(table, GENERATED_AT)

    assert table.query_args["KeyConditionExpression"] == "pk = :pk"
    assert table.query_args["ScanIndexForward"] is False
    assert table.query_args["Limit"] == 1
    assert table.query_args["ExpressionAttributeValues"][":pk"] == "TRENDING#1h"
    assert table.query_args["ExpressionAttributeNames"] == {"#ttl": "ttl"}


def test_latest_window_is_none_when_nothing_is_fresh() -> None:
    assert latest_window(FakeTable(items=[]), GENERATED_AT) is None


def test_numbers_come_back_as_plain_python_numbers() -> None:
    stored = {
        "items": [{"rank": Decimal(1), "score": Decimal(27)}],
        "ttl": Decimal(1793707200),
        "ratio": Decimal("0.5"),
    }

    assert plain(stored) == {
        "items": [{"rank": 1, "score": 27}],
        "ttl": 1793707200,
        "ratio": 0.5,
    }


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch) -> Iterator[Any]:
    """moto with fake credentials, so no test can reach a real AWS account."""
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "eu-west-2")
    with mock_aws():
        yield boto3.client("dynamodb")


def test_recreate_table_creates_an_empty_table_of_its_own(client: Any) -> None:
    definition = load_table_definition(DEFINITION)

    recreate_table(client, definition, "bench")
    client.put_item(
        TableName="bench", Item={"pk": {"S": "USER#u1"}, "sk": {"S": "prog-a"}}
    )
    recreate_table(client, definition, "bench")

    assert client.scan(TableName="bench")["Count"] == 0
    ttl = client.describe_time_to_live(TableName="bench")["TimeToLiveDescription"]
    assert ttl["TimeToLiveStatus"] == "ENABLED"
    # Only the table it was named, never the one in the definition.
    assert client.list_tables()["TableNames"] == ["bench"]
