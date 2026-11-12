from pathlib import Path

import pytest
from pydantic_settings import BaseSettings

from realtime_recommendation_pipeline.settings import (
    ContinueWatchingSettings,
    DynamoDBSettings,
    FlinkSettings,
    KafkaSettings,
    LogSettings,
    MetricsSettings,
    SettingsError,
    environment,
    load,
)

ROOT = Path(__file__).resolve().parents[1]
ENV_EXAMPLE = ROOT / ".env.example"
SETTINGS = (
    KafkaSettings,
    FlinkSettings,
    DynamoDBSettings,
    ContinueWatchingSettings,
    MetricsSettings,
    LogSettings,
)


def variable_names(settings_class: type[BaseSettings]) -> set[str]:
    prefix = settings_class.model_config.get("env_prefix", "")
    return {f"{prefix}{name}".upper() for name in settings_class.model_fields}


def documented_names() -> set[str]:
    lines = ENV_EXAMPLE.read_text().splitlines()
    return {
        line.split("=", 1)[0].strip()
        for line in lines
        if "=" in line and not line.lstrip().startswith("#")
    }


def test_env_example_documents_every_setting_and_nothing_else() -> None:
    expected = set().union(*(variable_names(cls) for cls in SETTINGS))

    assert documented_names() == expected


@pytest.mark.parametrize("settings_class", SETTINGS, ids=lambda cls: cls.__name__)
def test_env_example_alone_configures_every_component(
    settings_class: type[BaseSettings],
) -> None:
    settings = settings_class(_env_file=ENV_EXAMPLE)  # type: ignore[call-arg]

    assert None not in settings.model_dump().values()


def test_env_example_values_are_the_local_stack() -> None:
    dynamo = DynamoDBSettings(_env_file=ENV_EXAMPLE)  # type: ignore[call-arg]
    kafka = KafkaSettings(_env_file=ENV_EXAMPLE)  # type: ignore[call-arg]

    assert dynamo.table == "trending"
    assert dynamo.partition_key == "TRENDING#1h"  # quoted, so the # survives
    assert kafka.trending_topic == "trending-results"


def test_missing_settings_name_the_variables(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in variable_names(KafkaSettings):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "broker:9092")

    with pytest.raises(SettingsError) as raised:
        load(KafkaSettings)

    message = str(raised.value)
    assert "KAFKA_EVENTS_TOPIC" in message
    assert "KAFKA_WRITER_GROUP_ID" in message
    assert "KAFKA_BOOTSTRAP_SERVERS" not in message
    assert ".env.example" in message


def test_the_environment_wins_over_the_env_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("DYNAMODB_TABLE=from-file\nDYNAMODB_PARTITION_KEY=pk\n")
    monkeypatch.setenv("DYNAMODB_TABLE", "from-environment")

    settings = DynamoDBSettings(_env_file=env_file)  # type: ignore[call-arg]

    assert settings.table == "from-environment"
    assert settings.partition_key == "pk"


def test_a_blank_endpoint_means_aws(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DYNAMODB_ENDPOINT_URL", "")
    monkeypatch.setenv("DYNAMODB_TABLE", "trending")
    monkeypatch.setenv("DYNAMODB_PARTITION_KEY", "TRENDING#1h")

    assert load(DynamoDBSettings).endpoint_url is None


def test_environment_recreates_the_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    original = DynamoDBSettings(
        endpoint_url=None, table="chaos-table", partition_key="CHAOS#1h"
    )

    for name, value in environment(original).items():
        monkeypatch.setenv(name, value)

    assert load(DynamoDBSettings) == original
