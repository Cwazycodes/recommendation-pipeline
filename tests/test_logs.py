import json
import logging
from collections.abc import Iterator

import pytest

from realtime_recommendation_pipeline.common.logs import (
    configure_logging,
    service_logger,
)


@pytest.fixture
def restore_root_logger() -> Iterator[None]:
    root = logging.getLogger()
    handlers, level = root.handlers[:], root.level
    yield
    root.handlers[:] = handlers
    root.setLevel(level)


def test_lines_are_json_with_the_extra_fields(
    restore_root_logger: None, capsys: pytest.CaptureFixture[str]
) -> None:
    configure_logging("results-writer")

    logging.getLogger("writer").info(
        "wrote window",
        extra={"window_end": "2026-09-21T14:00:00Z", "programmes": 20, "error": None},
    )

    entry = json.loads(capsys.readouterr().err)
    assert entry["message"] == "wrote window"
    assert entry["level"] == "INFO"
    assert entry["service"] == "results-writer"
    assert entry["logger"] == "writer"
    assert entry["window_end"] == "2026-09-21T14:00:00Z"
    assert entry["programmes"] == 20
    assert "error" not in entry  # empty fields are left out
    assert entry["timestamp"].endswith("+00:00")


def test_debug_lines_appear_only_at_debug_level(
    restore_root_logger: None, capsys: pytest.CaptureFixture[str]
) -> None:
    configure_logging("producer", level="INFO")
    logging.getLogger("producer").debug("event delivered", extra={"event_id": "e-1"})
    assert capsys.readouterr().err == ""

    configure_logging("producer", level="DEBUG")
    logging.getLogger("producer").debug("event delivered", extra={"event_id": "e-1"})
    assert json.loads(capsys.readouterr().err)["event_id"] == "e-1"


def test_exceptions_carry_their_traceback(
    restore_root_logger: None, capsys: pytest.CaptureFixture[str]
) -> None:
    configure_logging("producer")

    try:
        raise RuntimeError("broker gone")
    except RuntimeError:
        logging.getLogger("producer").exception("producer error")

    entry = json.loads(capsys.readouterr().err)
    assert entry["level"] == "ERROR"
    assert "RuntimeError: broker gone" in entry["traceback"]


def test_a_service_logger_writes_to_stdout_on_its_own(
    capsys: pytest.CaptureFixture[str],
) -> None:
    logger = service_logger("tests.api", "trending-api", "INFO")

    logger.info("handled GET /trending", extra={"window_end": "2026-09-21T14:00:00Z"})

    captured = capsys.readouterr()
    assert json.loads(captured.out)["service"] == "trending-api"
    assert captured.err == ""
    assert logger.propagate is False
