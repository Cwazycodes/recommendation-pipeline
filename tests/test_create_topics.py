from pathlib import Path
from typing import Any

import pytest
from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import NewTopic

from realtime_recommendation_pipeline.settings import (
    ContinueWatchingSettings,
    KafkaSettings,
)
from realtime_recommendation_pipeline.tools import create_topics

ENV_EXAMPLE = Path(__file__).resolve().parents[1] / ".env.example"
KAFKA = KafkaSettings(_env_file=ENV_EXAMPLE)  # type: ignore[call-arg]
CONTINUE_WATCHING = ContinueWatchingSettings(_env_file=ENV_EXAMPLE)  # type: ignore[call-arg]


def partitions(topics: list[NewTopic]) -> dict[str, int]:
    return {topic.topic: topic.num_partitions for topic in topics}


class FakeFuture:
    def __init__(self, error: KafkaError | None) -> None:
        self.error = error

    def result(self, timeout: float) -> None:
        if self.error is not None:
            raise KafkaException(self.error)


class FakeAdminClient:
    """Already holds the topics in existing; fails with error for the rest."""

    def __init__(self, existing: set[str], error: int | None = None) -> None:
        self.existing = existing
        self.error = error
        self.requested: list[NewTopic] = []

    def __call__(self, config: dict[str, Any]) -> "FakeAdminClient":
        assert config == {"bootstrap.servers": KAFKA.bootstrap_servers}
        return self

    def create_topics(self, topics: list[NewTopic]) -> dict[str, FakeFuture]:
        self.requested = topics
        futures = {}
        for topic in topics:
            if topic.topic in self.existing:
                futures[topic.topic] = FakeFuture(
                    KafkaError(KafkaError.TOPIC_ALREADY_EXISTS)
                )
            elif self.error is not None:
                futures[topic.topic] = FakeFuture(KafkaError(self.error))
            else:
                futures[topic.topic] = FakeFuture(None)
        return futures


@pytest.fixture(autouse=True)
def settings_from_env_example(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(create_topics, "load", lambda cls: cls(_env_file=ENV_EXAMPLE))


def test_the_writers_read_one_partition_each() -> None:
    assert partitions(create_topics.pipeline_topics(KAFKA)) == {
        KAFKA.events_topic: 6,
        KAFKA.events_dlq_topic: 6,
        KAFKA.trending_topic: 1,
        KAFKA.writer_dlq_topic: 1,
    }
    assert partitions(create_topics.continue_watching_topics(CONTINUE_WATCHING)) == {
        CONTINUE_WATCHING.topic: 1,
        CONTINUE_WATCHING.writer_dlq_topic: 1,
    }


def test_main_creates_both_jobs_topics_and_skips_existing_ones(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    admin = FakeAdminClient(existing={KAFKA.events_topic})
    monkeypatch.setattr(create_topics, "AdminClient", admin)

    create_topics.main([])

    assert len(admin.requested) == 6
    output = capsys.readouterr().out.splitlines()
    assert f"Topic {KAFKA.events_topic} already exists" in output
    assert f"Created topic {CONTINUE_WATCHING.topic}" in output


def test_main_fails_on_any_other_error(monkeypatch: pytest.MonkeyPatch) -> None:
    admin = FakeAdminClient(existing=set(), error=KafkaError._TIMED_OUT)
    monkeypatch.setattr(create_topics, "AdminClient", admin)

    with pytest.raises(KafkaException):
        create_topics.main([])
