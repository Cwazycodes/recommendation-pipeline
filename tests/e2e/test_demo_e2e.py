"""The whole pipeline on the running Compose stack (make test-e2e)."""

import json
import os

import pytest

from realtime_recommendation_pipeline.tools.demo import (
    RESULTS_WITHIN_S,
    DemoSettings,
    run_demo,
)

pytestmark = pytest.mark.e2e


def test_trending_results_reach_dynamodb_within_the_agreed_time() -> None:
    settings = DemoSettings(
        bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"),
        flink_url=os.environ.get("FLINK_URL", "http://localhost:8081"),
        dynamodb_endpoint=os.environ.get("DYNAMODB_ENDPOINT", "http://localhost:8001"),
        # Long enough for the watermark to pass the first windows.
        duration_s=60,
    )

    result = run_demo(settings)

    assert result.first_window_after_s is not None, "no window reached DynamoDB"
    assert result.first_window_after_s <= RESULTS_WITHIN_S
    assert result.response["statusCode"] == 200
    body = json.loads(result.response["body"])
    assert 1 <= len(body["items"]) <= settings.limit
    assert [item["rank"] for item in body["items"]] == list(
        range(1, len(body["items"]) + 1)
    )
