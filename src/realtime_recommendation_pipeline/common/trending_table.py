"""The 'trending' DynamoDB table: one item per window, newest last.

The API's only question is "what is trending now?", which is the item with
the highest window_end under one partition key. A single Query that reads the
partition backwards with Limit=1 answers it.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import boto3
from botocore.config import Config

PARTITION_KEY = "TRENDING#1h"
RETENTION = timedelta(hours=24)
TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

# DynamoDB Local hangs instead of failing when it cannot open its database,
# so fail fast rather than let boto3 retry for minutes.
LOCAL_CLIENT_CONFIG = Config(
    connect_timeout=2, read_timeout=10, retries={"max_attempts": 3}
)


def load_table_definition(path: Path) -> dict[str, Any]:
    """The table definition that DynamoDB Local and Terraform both use."""
    definition: dict[str, Any] = json.loads(path.read_text())
    return definition


def dynamodb_resource(endpoint_url: str | None = None) -> Any:
    """boto3 DynamoDB resource; pass endpoint_url to use DynamoDB Local."""
    if endpoint_url is None:
        return boto3.resource("dynamodb")

    # With -sharedDb, DynamoDB Local accepts any credentials and region.
    return boto3.resource(
        "dynamodb",
        endpoint_url=endpoint_url,
        region_name="us-east-1",
        aws_access_key_id="local",
        aws_secret_access_key="local",
        config=LOCAL_CLIENT_CONFIG,
    )


def create_table_if_missing(client: Any, definition: Mapping[str, Any]) -> bool:
    """Create the table and switch on TTL. Returns False if it already existed."""
    name = definition["TableName"]
    create_args = {
        key: value
        for key, value in definition.items()
        if key != "TimeToLiveSpecification"
    }

    try:
        client.create_table(**create_args)
        created = True
    except client.exceptions.ResourceInUseException:
        created = False

    client.get_waiter("table_exists").wait(TableName=name)

    # Enabling TTL twice is an error, so only do it when it is off.
    ttl = client.describe_time_to_live(TableName=name)["TimeToLiveDescription"]
    if ttl.get("TimeToLiveStatus") not in ("ENABLED", "ENABLING"):
        client.update_time_to_live(
            TableName=name,
            TimeToLiveSpecification=definition["TimeToLiveSpecification"],
        )

    return created


def window_item(
    window_end: str,
    items: Sequence[Mapping[str, Any]],
    generated_at: datetime,
    partition_key: str = PARTITION_KEY,
) -> dict[str, Any]:
    """Table item for one window's ranked list.

    ttl counts from generated_at, the wall-clock time the list was written,
    not from window_end, which for replayed or synthetic data can already be
    days old.
    """
    return {
        "pk": partition_key,
        "sk": window_end,
        "items": [dict(item) for item in items],
        "generated_at": generated_at.strftime(TIME_FORMAT),
        "ttl": int((generated_at + RETENTION).timestamp()),
    }


def put_window(table: Any, item: Mapping[str, Any]) -> None:
    """Write a window; writing the same window again replaces it."""
    table.put_item(Item=dict(item))


def latest_window(
    table: Any, now: datetime, partition_key: str = PARTITION_KEY
) -> dict[str, Any] | None:
    """The newest window's item, or None if there is none or it has expired."""
    response = table.query(
        KeyConditionExpression="pk = :pk",
        # DynamoDB deletes expired items lazily (AWS: within about two days),
        # so expired ones are filtered out. ttl is a reserved word.
        FilterExpression="#ttl > :now",
        ExpressionAttributeNames={"#ttl": "ttl"},
        ExpressionAttributeValues={":pk": partition_key, ":now": int(now.timestamp())},
        ScanIndexForward=False,
        Limit=1,
    )
    items = response["Items"]

    return plain(items[0]) if items else None


def plain(value: Any) -> Any:
    """Turn the Decimals DynamoDB returns back into ints and floats."""
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, list):
        return [plain(entry) for entry in value]
    if isinstance(value, dict):
        return {key: plain(entry) for key, entry in value.items()}

    return value
