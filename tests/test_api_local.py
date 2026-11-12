from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import boto3
import pytest
from moto import mock_aws

from realtime_recommendation_pipeline.api.handler import configured
from realtime_recommendation_pipeline.common.continue_watching_table import (
    progress_item,
)
from realtime_recommendation_pipeline.common.trending_table import (
    create_table_if_missing,
    load_table_definition,
)
from realtime_recommendation_pipeline.tools.api_local import http_api_event, main

ROOT = Path(__file__).resolve().parents[1]
DEFINITION = load_table_definition(ROOT / "infra" / "dynamodb" / "trending-table.json")


@pytest.fixture
def table(monkeypatch: pytest.MonkeyPatch) -> Iterator[Any]:
    """moto with fake credentials, so no test can reach a real AWS account.

    The DYNAMODB_* variables are set here so the ones call_handler writes are
    put back afterwards.
    """
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


def test_continue_watching_event_names_the_route_and_the_user() -> None:
    event = http_api_event({"limit": "5"}, "user-00040")

    assert event["routeKey"] == "GET /users/{id}/continue-watching"
    assert event["requestContext"]["routeKey"] == event["routeKey"]
    assert event["rawPath"] == "/users/user-00040/continue-watching"
    assert event["requestContext"]["http"]["path"] == event["rawPath"]
    assert event["pathParameters"] == {"id": "user-00040"}
    assert event["queryStringParameters"] == {"limit": "5"}
    assert event["rawQueryString"] == "limit=5"


def test_trending_event_has_no_path_or_query_parameters() -> None:
    event = http_api_event({})

    assert event["routeKey"] == "GET /trending"
    assert event["rawPath"] == "/trending"
    assert "pathParameters" not in event
    assert "queryStringParameters" not in event


def test_main_prints_the_users_list(
    table: Any, capsys: pytest.CaptureFixture[str]
) -> None:
    now_ms = int(datetime.now(UTC).timestamp() * 1000)
    table.put_item(
        Item=progress_item(
            {
                "user_id": "user-00040",
                "programme_id": "programme-0001",
                "position_s": 600.0,
                "duration_s": 3600.0,
                "event_time": now_ms,
                "event_id": "e1",
                "emitted_at": now_ms,
            }
        )
    )

    with pytest.raises(SystemExit) as exit_info:
        main(["--user", "user-00040"])

    assert exit_info.value.code == 0
    output = capsys.readouterr().out
    assert "HTTP 200\n" in output
    assert '"programme_id": "programme-0001"' in output


def test_main_exits_1_when_the_request_is_refused(
    table: Any, capsys: pytest.CaptureFixture[str]
) -> None:
    with pytest.raises(SystemExit) as exit_info:
        main(["--user", "user 40"])

    assert exit_info.value.code == 1
    assert "HTTP 400\n" in capsys.readouterr().out
