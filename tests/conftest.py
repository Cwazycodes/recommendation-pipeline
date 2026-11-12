"""Shared setup for every test."""

from collections.abc import Iterator

import pytest

from realtime_recommendation_pipeline.settings import (
    ContinueWatchingSettings,
    DynamoDBSettings,
    FlinkSettings,
    KafkaSettings,
    LogSettings,
    MetricsSettings,
    SchemaRegistrySettings,
)


@pytest.fixture(autouse=True)
def no_env_file(
    request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch
) -> Iterator[None]:
    """Settings come only from what a test sets, never from a developer's .env.

    End-to-end and registry tests run against the local stack, so they keep
    reading .env.
    """
    if not any(request.node.get_closest_marker(m) for m in ("e2e", "registry")):
        for settings_class in (
            KafkaSettings,
            FlinkSettings,
            DynamoDBSettings,
            ContinueWatchingSettings,
            SchemaRegistrySettings,
            MetricsSettings,
            LogSettings,
        ):
            monkeypatch.setitem(settings_class.model_config, "env_file", None)
    yield
