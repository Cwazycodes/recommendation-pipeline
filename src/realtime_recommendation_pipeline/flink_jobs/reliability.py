"""Checkpointing, state backend and restart settings for the streaming job."""

from __future__ import annotations

from collections.abc import Mapping

from pyflink.common import Configuration

CHECKPOINT_INTERVAL_S = 10
CHECKPOINT_TIMEOUT_S = 120

TRANSACTION_TIMEOUT_MS = 10 * 60 * 1_000
BROKER_MAX_TRANSACTION_TIMEOUT_MS = 15 * 60 * 1_000


def job_settings(checkpoint_dir: str) -> dict[str, str]:
    """Checkpoints, the RocksDB state backend and the restart strategy.

    checkpoint_dir is FLINK_CHECKPOINT_DIR: the flink-checkpoints volume, as the
    JobManager and TaskManager mount it. Savepoints go to its savepoints folder.
    """
    return {
        # Exactly-once checkpoints every 10 s, kept if the job is cancelled.
        "execution.checkpointing.interval": f"{CHECKPOINT_INTERVAL_S} s",
        "execution.checkpointing.timeout": f"{CHECKPOINT_TIMEOUT_S} s",
        "execution.checkpointing.mode": "EXACTLY_ONCE",
        "execution.checkpointing.externalized-checkpoint-retention": (
            "RETAIN_ON_CANCELLATION"
        ),
        # Keyed state lives in RocksDB; checkpoints go to the mounted volume.
        "state.backend.type": "rocksdb",
        "execution.checkpointing.incremental": "true",
        "execution.checkpointing.storage": "filesystem",
        "execution.checkpointing.dir": checkpoint_dir,
        "execution.checkpointing.savepoint-dir": f"{checkpoint_dir}/savepoints",
        # After a failure wait 1 s, 2 s, 4 s ... up to 1 min between restarts.
        "restart-strategy.type": "exponential-delay",
        "restart-strategy.exponential-delay.initial-backoff": "1 s",
        "restart-strategy.exponential-delay.max-backoff": "1 min",
        "restart-strategy.exponential-delay.backoff-multiplier": "2.0",
        "restart-strategy.exponential-delay.reset-backoff-threshold": "10 min",
        "restart-strategy.exponential-delay.jitter-factor": "0.1",
    }


def to_flink_configuration(settings: Mapping[str, str]) -> Configuration:
    """Flink Configuration holding the given settings."""
    configuration = Configuration()

    for key, value in settings.items():
        configuration.set_string(key, value)

    return configuration
