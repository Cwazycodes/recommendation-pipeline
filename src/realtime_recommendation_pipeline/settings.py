"""Every setting that differs between machines, read from the environment.

Values come from environment variables or, failing that, from a .env file in
the working directory; make install copies .env.example to .env. No host,
port, topic, consumer group, table or key has a default here, so a missing
variable is an error that names it instead of a silent fallback to a
local address. The one default is the log level.

Each component loads only the class it needs, so the Lambda handler, for one,
needs nothing but the DynamoDB variables. This module imports nothing else
from the package, so the Flink job can load it without boto3.
"""

from __future__ import annotations

from typing import TypeVar

from pydantic import ValidationError, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

ENV_FILE = ".env"

Settings = TypeVar("Settings", bound=BaseSettings)


class SettingsError(RuntimeError):
    """Settings are missing or invalid; the message names the variables."""


def settings_config(env_prefix: str = "") -> SettingsConfigDict:
    return SettingsConfigDict(
        env_prefix=env_prefix, env_file=ENV_FILE, extra="ignore", frozen=True
    )


class KafkaSettings(BaseSettings):
    """Kafka as the producer, the results writer and the tools reach it."""

    model_config = settings_config("KAFKA_")

    bootstrap_servers: str
    events_topic: str
    events_dlq_topic: str
    trending_topic: str
    writer_dlq_topic: str
    writer_group_id: str
    writer_instance_id: str


class FlinkSettings(BaseSettings):
    """The Flink cluster: its REST API, and what the job needs inside it."""

    model_config = settings_config("FLINK_")

    rest_url: str
    # Kafka as the job reaches it from inside the Compose network.
    kafka_bootstrap_servers: str
    source_group_id: str
    checkpoint_dir: str


class DynamoDBSettings(BaseSettings):
    """The trending table. No endpoint means AWS itself."""

    model_config = settings_config("DYNAMODB_")

    endpoint_url: str | None = None
    table: str
    partition_key: str

    @field_validator("endpoint_url", mode="before")
    @classmethod
    def blank_means_aws(cls, value: object) -> object:
        return None if value == "" else value


class LogSettings(BaseSettings):
    model_config = settings_config()

    log_level: str = "INFO"


def load(settings_class: type[Settings]) -> Settings:
    """Read settings, naming the environment variables that are missing."""
    try:
        return settings_class()
    except ValidationError as exc:
        prefix = settings_class.model_config.get("env_prefix", "")
        names = sorted({f"{prefix}{error['loc'][0]}".upper() for error in exc.errors()})
        raise SettingsError(
            f"Missing or invalid settings: {', '.join(names)}. Copy .env.example "
            "to .env (make install does this) or set them in the environment."
        ) from exc


def environment(settings: BaseSettings) -> dict[str, str]:
    """The environment variables that recreate settings, e.g. in a subprocess."""
    prefix = settings.model_config.get("env_prefix", "")
    return {
        f"{prefix}{name}".upper(): "" if value is None else str(value)
        for name, value in settings.model_dump().items()
    }
