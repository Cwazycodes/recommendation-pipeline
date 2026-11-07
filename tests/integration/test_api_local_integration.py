"""The local runner calls the Lambda handler against DynamoDB Local."""

import json
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from realtime_recommendation_pipeline.api.handler import configured
from realtime_recommendation_pipeline.common.trending_table import (
    create_table_if_missing,
    load_table_definition,
    put_window,
    window_item,
)
from realtime_recommendation_pipeline.tools.api_local import main

pytestmark = pytest.mark.integration

DEFINITION = load_table_definition(
    Path(__file__).resolve().parents[2] / "infra" / "dynamodb" / "trending-table.json"
)


def ranked(count: int, top_score: int = 100) -> list[dict[str, Any]]:
    return [
        {
            "rank": rank,
            "programme_id": f"programme-{rank:04d}",
            "score": top_score - rank,
        }
        for rank in range(1, count + 1)
    ]


@pytest.fixture(autouse=True)
def local_settings(dynamodb: Any, monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """The runner's DYNAMODB_* settings, pointing at the DynamoDB Local container.

    main() passes them on to the handler through the environment, as Lambda
    does; monkeypatch puts them back afterwards.
    """
    monkeypatch.setenv("DYNAMODB_ENDPOINT_URL", dynamodb.meta.client.meta.endpoint_url)
    monkeypatch.setenv("DYNAMODB_TABLE", "trending")
    monkeypatch.setenv("DYNAMODB_PARTITION_KEY", "TRENDING#1h")
    yield
    configured.cache_clear()


def run(argv: list[str], capsys: pytest.CaptureFixture[str]) -> tuple[int, str, Any]:
    """main()'s exit code, and the status line and body it printed."""
    with pytest.raises(SystemExit) as exited:
        main(argv)

    log_line, status, *body = capsys.readouterr().out.splitlines()
    assert json.loads(log_line)["message"] == "handled GET /trending"

    return int(str(exited.value.code)), status, json.loads("\n".join(body))


def test_runner_returns_the_latest_top_n(
    dynamodb: Any, capsys: pytest.CaptureFixture[str]
) -> None:
    table = dynamodb.Table("trending")
    now = datetime.now(UTC)
    put_window(table, window_item("2026-09-21T18:05:00Z", ranked(20, 50), now))
    put_window(table, window_item("2026-09-21T18:10:00Z", ranked(20), now))

    code, status, body = run(["--limit", "5"], capsys)

    assert (code, status) == (0, "HTTP 200")
    assert body["window_end"] == "2026-09-21T18:10:00Z"
    assert body["items"] == ranked(5)


def test_runner_shows_the_404_when_there_is_no_data(
    dynamodb: Any, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    empty = {**DEFINITION, "TableName": "trending-empty"}
    create_table_if_missing(dynamodb.meta.client, empty)
    monkeypatch.setenv("DYNAMODB_TABLE", "trending-empty")

    code, status, body = run([], capsys)

    assert (code, status) == (1, "HTTP 404")
    assert body["error"] == "no_trending_data"
