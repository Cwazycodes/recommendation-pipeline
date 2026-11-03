import json
import subprocess
import sys
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import boto3
import pytest
from moto import mock_aws

from realtime_recommendation_pipeline.api.handler import (
    API_CLIENT_CONFIG,
    default_table,
    handle,
    handler,
)
from realtime_recommendation_pipeline.common.trending_table import (
    create_table_if_missing,
    dynamodb_resource,
    load_table_definition,
    put_window,
    window_item,
)

ROOT = Path(__file__).resolve().parents[1]
DEFINITION = load_table_definition(ROOT / "infra" / "dynamodb" / "trending-table.json")
EVENT = Path(__file__).resolve().parent / "fixtures" / "http_api_get_trending.json"
NOW = datetime(2026, 11, 3, 12, 0, tzinfo=UTC)


def ranked(count: int, top_score: int = 100) -> list[dict[str, Any]]:
    return [
        {
            "rank": rank,
            "programme_id": f"programme-{rank:04d}",
            "score": top_score - rank,
        }
        for rank in range(1, count + 1)
    ]


@pytest.fixture
def aws(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """moto with fake credentials, so no test can reach a real AWS account."""
    for name in ("AWS_PROFILE", "DYNAMODB_ENDPOINT_URL"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "eu-west-2")
    monkeypatch.setenv("TABLE_NAME", "trending")

    default_table.cache_clear()
    with mock_aws():
        yield
    default_table.cache_clear()


@pytest.fixture
def table(aws: None) -> Any:
    """The trending table, created from the same definition as DynamoDB Local."""
    dynamodb = boto3.resource("dynamodb")
    create_table_if_missing(dynamodb.meta.client, DEFINITION)
    return dynamodb.Table("trending")


@pytest.fixture
def event() -> dict[str, Any]:
    """An API Gateway HTTP API (payload 2.0) event for GET /trending?limit=10."""
    loaded: dict[str, Any] = json.loads(EVENT.read_text())
    return loaded


def with_query(event: dict[str, Any], query: dict[str, str]) -> dict[str, Any]:
    event = {**event, "rawQueryString": "&".join(f"{k}={v}" for k, v in query.items())}
    if query:
        event["queryStringParameters"] = query
    else:
        event.pop("queryStringParameters", None)
    return event


def body(response: dict[str, Any]) -> Any:
    assert response["headers"]["content-type"] == "application/json"
    return json.loads(response["body"])


def test_returns_the_top_n_of_the_latest_window(
    table: Any, event: dict[str, Any]
) -> None:
    generated_at = NOW - timedelta(minutes=1)
    put_window(table, window_item("2026-09-21T18:05:00Z", ranked(20, 50), generated_at))
    put_window(table, window_item("2026-09-21T18:10:00Z", ranked(20), generated_at))

    response = handle(event, table, NOW)

    assert response["statusCode"] == 200
    assert response["isBase64Encoded"] is False
    assert body(response) == {
        "window_end": "2026-09-21T18:10:00Z",
        "generated_at": "2026-11-03T11:59:00Z",
        "limit": 10,
        "items": ranked(10),
    }
    assert list(body(response)["items"][0]) == ["rank", "programme_id", "score"]


@pytest.mark.parametrize(
    ("query", "expected_items"),
    [({}, 10), ({"limit": "1"}, 1), ({"limit": "20"}, 20), ({"limit": "07"}, 7)],
    ids=["default", "one", "max", "leading-zero"],
)
def test_limit_sets_how_many_programmes_come_back(
    table: Any, event: dict[str, Any], query: dict[str, str], expected_items: int
) -> None:
    put_window(table, window_item("2026-09-21T18:10:00Z", ranked(20), NOW))

    response = handle(with_query(event, query), table, NOW)

    assert response["statusCode"] == 200
    assert body(response)["items"] == ranked(expected_items)


def test_a_short_list_is_returned_whole(table: Any, event: dict[str, Any]) -> None:
    put_window(table, window_item("2026-09-21T18:10:00Z", ranked(3), NOW))

    response = handle(event, table, NOW)

    assert body(response)["items"] == ranked(3)


@pytest.mark.parametrize("limit", ["0", "21", "-1", "abc", "1.5", "", " 5", "5,6"])
def test_invalid_limit_is_a_400(table: Any, event: dict[str, Any], limit: str) -> None:
    response = handle(with_query(event, {"limit": limit}), table, NOW)

    assert response["statusCode"] == 400
    assert body(response) == {
        "error": "invalid_limit",
        "message": "limit must be a whole number from 1 to 20.",
    }


def test_unknown_query_parameter_is_a_400(table: Any, event: dict[str, Any]) -> None:
    response = handle(with_query(event, {"limt": "5"}), table, NOW)

    assert response["statusCode"] == 400
    assert body(response)["error"] == "unknown_parameter"
    assert "limt" in body(response)["message"]


def test_no_data_yet_is_a_404_json_body(table: Any, event: dict[str, Any]) -> None:
    response = handle(event, table, NOW)

    assert response["statusCode"] == 404
    assert body(response) == {
        "error": "no_trending_data",
        "message": "No trending results are available yet.",
    }


def test_expired_window_is_not_served(table: Any, event: dict[str, Any]) -> None:
    put_window(
        table,
        window_item("2026-09-21T18:10:00Z", ranked(20), NOW - timedelta(hours=25)),
    )

    response = handle(event, table, NOW)

    assert response["statusCode"] == 404


def test_only_get_is_allowed(table: Any, event: dict[str, Any]) -> None:
    event["requestContext"]["http"]["method"] = "POST"

    response = handle(event, table, NOW)

    assert response["statusCode"] == 405
    assert response["headers"]["allow"] == "GET"
    assert body(response)["error"] == "method_not_allowed"


def test_each_request_logs_one_json_line(
    table: Any, event: dict[str, Any], capsys: pytest.CaptureFixture[str]
) -> None:
    put_window(table, window_item("2026-09-21T18:10:00Z", ranked(20), NOW))

    handle(event, table, NOW, lambda_request_id="lambda-123")

    (line,) = capsys.readouterr().out.splitlines()
    entry = json.loads(line)
    assert entry["level"] == "INFO"
    assert entry["request_id"] == "KxYz1jAbLPEEJ2A="
    assert entry["lambda_request_id"] == "lambda-123"
    assert entry["method"] == "GET"
    assert entry["query"] == "limit=10"
    assert entry["status"] == 200
    assert entry["window_end"] == "2026-09-21T18:10:00Z"
    assert entry["items"] == 10
    assert entry["duration_ms"] >= 0
    assert "error" not in entry


def test_missing_table_is_a_logged_500(
    aws: None, event: dict[str, Any], capsys: pytest.CaptureFixture[str]
) -> None:
    missing = boto3.resource("dynamodb").Table("no-such-table")

    response = handle(event, missing, NOW)

    assert response["statusCode"] == 500
    assert body(response) == {
        "error": "internal_error",
        "message": "Trending results could not be read.",
    }
    entry = json.loads(capsys.readouterr().out)
    assert entry["level"] == "ERROR"
    assert entry["status"] == 500
    assert entry["exception"] == "ResourceNotFoundException"


def test_unreachable_dynamodb_is_a_logged_500(
    event: dict[str, Any], capsys: pytest.CaptureFixture[str]
) -> None:
    # Nothing listens on port 9, so the connection is refused straight away.
    dynamodb = dynamodb_resource("http://127.0.0.1:9", config=API_CLIENT_CONFIG)

    response = handle(event, dynamodb.Table("trending"), NOW)

    assert response["statusCode"] == 500
    assert json.loads(capsys.readouterr().out)["exception"] == "EndpointConnectionError"


def test_a_bug_is_raised_rather_than_hidden_as_a_500(event: dict[str, Any]) -> None:
    class ItemWithoutGeneratedAt:
        def query(self, **kwargs: Any) -> dict[str, Any]:
            return {"Items": [{"pk": "TRENDING#1h", "sk": "2026-09-21T18:10:00Z"}]}

    with pytest.raises(KeyError):
        handle(event, ItemWithoutGeneratedAt(), NOW)


def test_lambda_entry_point_reads_the_table_named_in_the_environment(
    table: Any, event: dict[str, Any]
) -> None:
    put_window(
        table, window_item("2026-09-21T18:10:00Z", ranked(20), datetime.now(UTC))
    )

    response = handler(event, SimpleNamespace(aws_request_id="lambda-123"))

    assert response["statusCode"] == 200
    assert body(response)["window_end"] == "2026-09-21T18:10:00Z"


def test_handler_imports_nothing_the_lambda_runtime_lacks() -> None:
    # The Lambda Python runtime ships boto3 but not the pipeline's other
    # dependencies, so the handler must not pull them in.
    script = (
        "import sys\n"
        "import realtime_recommendation_pipeline.api.handler\n"
        "print(sorted(m for m in ('confluent_kafka', 'pydantic', 'pyflink') "
        "if m in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, check=True
    )

    assert result.stdout.strip() == "[]"
