import pytest
from pyflink.datastream import (
    CheckpointingMode,
    ExternalizedCheckpointCleanup,
    StreamExecutionEnvironment,
)

from realtime_recommendation_pipeline.flink_jobs.reliability import (
    BROKER_MAX_TRANSACTION_TIMEOUT_MS,
    CHECKPOINT_INTERVAL_S,
    CHECKPOINT_TIMEOUT_S,
    TRANSACTION_TIMEOUT_MS,
    job_settings,
    to_flink_configuration,
)


def test_checkpoints_go_to_rocksdb_and_the_mounted_volume() -> None:
    settings = job_settings()

    assert settings["execution.checkpointing.interval"] == "10 s"
    assert settings["execution.checkpointing.mode"] == "EXACTLY_ONCE"
    assert settings["state.backend.type"] == "rocksdb"
    assert settings["execution.checkpointing.dir"] == "file:///opt/flink/checkpoints"


def test_restarts_back_off_exponentially() -> None:
    settings = job_settings()

    assert settings["restart-strategy.type"] == "exponential-delay"
    assert settings["restart-strategy.exponential-delay.backoff-multiplier"] == "2.0"


def test_transaction_timeout_fits_under_the_broker_limit() -> None:
    assert TRANSACTION_TIMEOUT_MS < BROKER_MAX_TRANSACTION_TIMEOUT_MS
    # A transaction stays open for one interval plus the checkpoint itself.
    assert (
        TRANSACTION_TIMEOUT_MS > (CHECKPOINT_INTERVAL_S + CHECKPOINT_TIMEOUT_S) * 1_000
    )


@pytest.mark.slow
def test_flink_parses_the_checkpoint_settings() -> None:
    env = StreamExecutionEnvironment.get_execution_environment(
        to_flink_configuration(job_settings())
    )
    checkpoints = env.get_checkpoint_config()

    assert checkpoints.get_checkpoint_interval() == 10_000
    assert checkpoints.get_checkpoint_timeout() == 120_000
    assert checkpoints.get_checkpointing_mode() == CheckpointingMode.EXACTLY_ONCE
    assert (
        checkpoints.get_externalized_checkpoint_cleanup()
        == ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
    )
