import json
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import boto3
import pytest
from moto import mock_aws

from realtime_recommendation_pipeline.api.handler import configured, handle, handler
from realtime_recommendation_pipeline.common.continue_watching_table import (
    progress_item,
)
from realtime_recommendation_pipeline.common.trending_table import (
    create_table_if_missing,
    load_table_definition,
    put_window,
    window_item,
)

ROOT = Path(__file__).resolve().parents[1]
DEFINITION = load_table_definition(ROOT / "infra" / "dynamodb" / "trending-table.json")
TRENDING_EVENT = (
    Path(__file__).resolve().parent / "fixtures" / "http_api_get_trending.json"
)
NOW = datetime(2026, 11, 3, 12, 0, tzinfo=UTC)
NOW_MS = int(NOW.timestamp() * 1000)


def upsert(
    user_id: str, programme_id: str, position_s: float, watched_minutes_ago: int
) -> dict[str, Any]:
    event_time = NOW_MS - watched_minutes_ago * 60_000
    return {
        "op": "upsert",
        "user_id": user_id,
        "programme_id": programme_id,
        "position_s": position_s,
        "duration_s": 3600.0,
        "event_time": event_time,
        "event_id": f"{user_id}-{programme_id}",
        "produced_at": event_time,
        "emitted_at": event_time + 5_000,
    }


@pytest.fixture
def table(monkeypatch: pytest.MonkeyPatch) -> Iterator[Any]:
    """moto with fake credentials, so no test can reach a real AWS account."""
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "eu-west-2")
    monkeypatch.setenv("DYNAMODB_ENDPOINT_URL", "")
    monkeypatch.setenv("DYNAMODB_TABLE", "trending")
    monkeypatch.setenv("DYNAMODB_PARTITION_KEY", "TRENDING#1h")

    configured.cache_clear()
    with mock_aws():
        dynamodb = boto3.resource("dynamodb")
        create_table_if_missing(dynamodb.meta.client, DEFINITION)
        yield dynamodb.Table("trending")
    configured.cache_clear()


def request(user_id: str, query: dict[str, str] | None = None) -> dict[str, Any]:
    """API Gateway's event for GET /users/{id}/continue-watching."""
    event: dict[str, Any] = json.loads(TRENDING_EVENT.read_text())
    path = f"/users/{user_id}/continue-watching"
    event["routeKey"] = "GET /users/{id}/continue-watching"
    event["rawPath"] = path
    event["requestContext"]["routeKey"] = event["routeKey"]
    event["requestContext"]["http"]["path"] = path
    event["pathParameters"] = {"id": user_id}
    event["rawQueryString"] = "&".join(f"{k}={v}" for k, v in (query or {}).items())
    event.pop("queryStringParameters", None)
    if query:
        event["queryStringParameters"] = query
    return event


def body(response: dict[str, Any]) -> dict[str, Any]:
    loaded: dict[str, Any] = json.loads(response["body"])
    return loaded


def test_returns_the_users_programmes_most_recently_watched_first(table: Any) -> None:
    for item in (
        upsert("user-00040", "programme-0001", 600.0, watched_minutes_ago=90),
        upsert("user-00040", "programme-0002", 1234.5, watched_minutes_ago=5),
        upsert("user-00041", "programme-0003", 60.0, watched_minutes_ago=1),
    ):
        table.put_item(Item=progress_item(item))

    response = handle(request("user-00040"), table, NOW)

    assert response["statusCode"] == 200
    assert body(response) == {
        "user_id": "user-00040",
        "limit": 10,
        "items": [
            {
                "programme_id": "programme-0002",
                "position_s": 1234.5,
                "duration_s": 3600,
                "progress": 0.3429,
                "last_watched_at": "2026-11-03T11:55:00.000Z",
            },
            {
                "programme_id": "programme-0001",
                "position_s": 600,
                "duration_s": 3600,
                "progress": 0.1667,
                "last_watched_at": "2026-11-03T10:30:00.000Z",
            },
        ],
    }


def test_limit_sets_how_many_come_back(table: Any) -> None:
    for minutes in range(5):
        table.put_item(
            Item=progress_item(upsert("u1", f"programme-{minutes:04d}", 60.0, minutes))
        )

    response = handle(request("u1", {"limit": "2"}), table, NOW)

    assert [item["programme_id"] for item in body(response)["items"]] == [
        "programme-0000",
        "programme-0001",
    ]


def test_a_user_with_nothing_in_progress_gets_an_empty_list(table: Any) -> None:
    put_window(
        table, window_item("2026-11-03T11:55:00Z", [], NOW, partition_key="TRENDING#1h")
    )

    response = handle(request("user-99999"), table, NOW)

    assert response["statusCode"] == 200
    assert body(response) == {"user_id": "user-99999", "limit": 10, "items": []}


def test_expired_items_are_not_served(table: Any) -> None:
    table.put_item(Item=progress_item(upsert("u1", "programme-0001", 60.0, 0)))

    response = handle(request("u1"), table, NOW + timedelta(days=31))

    assert body(response)["items"] == []


@pytest.mark.parametrize("user_id", ["", "user 1", "user/1", "u" * 65, "USER#u1"])
def test_invalid_user_id_is_a_400(table: Any, user_id: str) -> None:
    response = handle(request(user_id), table, NOW)

    assert response["statusCode"] == 400
    assert body(response)["error"] == "invalid_user_id"


@pytest.mark.parametrize("limit", ["0", "51", "ten", "5,6"])
def test_invalid_limit_is_a_400(table: Any, limit: str) -> None:
    response = handle(request("u1", {"limit": limit}), table, NOW)

    assert response["statusCode"] == 400
    assert body(response) == {
        "error": "invalid_limit",
        "message": "limit must be a whole number from 1 to 50.",
    }


def test_unknown_query_parameter_is_a_400(table: Any) -> None:
    response = handle(request("u1", {"offset": "5"}), table, NOW)

    assert body(response)["error"] == "unknown_parameter"


def test_each_request_logs_one_json_line(
    table: Any, capsys: pytest.CaptureFixture[str]
) -> None:
    table.put_item(Item=progress_item(upsert("u1", "programme-0001", 60.0, 0)))

    handle(request("u1"), table, NOW, lambda_request_id="lambda-1")

    entry = json.loads(capsys.readouterr().out)
    assert entry["message"] == "handled GET /users/{id}/continue-watching"
    assert entry["status"] == 200
    assert entry["user_id"] == "u1"
    assert entry["items"] == 1


def test_a_dynamodb_error_is_a_logged_500(
    table: Any, capsys: pytest.CaptureFixture[str]
) -> None:
    missing = boto3.resource("dynamodb").Table("no-such-table")

    response = handle(request("u1"), missing, NOW)

    assert response["statusCode"] == 500
    assert body(response) == {
        "error": "internal_error",
        "message": "The continue-watching list could not be read.",
    }
    assert json.loads(capsys.readouterr().out)["level"] == "ERROR"


def test_lambda_entry_point_serves_the_route(table: Any) -> None:
    table.put_item(
        Item=progress_item(upsert("u1", "programme-0001", 60.0, 0))
        | {"ttl": int((datetime.now(UTC) + timedelta(days=1)).timestamp())}
    )

    response = handler(request("u1"), SimpleNamespace(aws_request_id="lambda-1"))

    assert response["statusCode"] == 200
    assert [item["programme_id"] for item in body(response)["items"]] == [
        "programme-0001"
    ]
