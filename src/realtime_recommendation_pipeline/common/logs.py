"""Structured JSON logs: one JSON object per line.

Every service logs through the standard logging module; fields passed with
extra=... become keys of the line. Lines about a single event carry its
event_id and lines about a window carry its window_end, so one result can be
followed from the producer to the API by those two keys.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import UTC, datetime
from typing import Any, TextIO

# Attributes every LogRecord has; anything else on a record came from extra=.
STANDARD_ATTRIBUTES = frozenset(vars(logging.makeLogRecord({}))) | {
    "message",
    "asctime",
}


class JsonFormatter(logging.Formatter):
    def __init__(self, service: str) -> None:
        super().__init__()
        self.service = service

    def format(self, record: logging.LogRecord) -> str:
        entry: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, UTC).isoformat(
                timespec="milliseconds"
            ),
            "level": record.levelname,
            "service": self.service,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key, value in vars(record).items():
            if key not in STANDARD_ATTRIBUTES and value is not None:
                entry[key] = value
        if record.exc_info:
            entry["traceback"] = self.formatException(record.exc_info)

        return json.dumps(entry, default=str)


class CurrentStreamHandler(logging.Handler):
    """Writes to whatever sys.stdout or sys.stderr is when a line is logged.

    logging.StreamHandler keeps the stream it was created with, which misses
    a stream swapped later, as pytest's capsys does.
    """

    def __init__(self, stream_name: str) -> None:
        super().__init__()
        self.stream_name = stream_name

    def emit(self, record: logging.LogRecord) -> None:
        try:
            stream: TextIO = getattr(sys, self.stream_name)
            stream.write(self.format(record) + "\n")
            stream.flush()
        except Exception:  # noqa: BLE001 - logging must never raise
            self.handleError(record)


def service_logger(
    name: str, service: str, level: str, stream: str = "stdout"
) -> logging.Logger:
    """A logger that writes its own JSON lines, whatever the root logger does.

    For code that does not own the process's logging, like the Lambda handler:
    the Lambda runtime installs its own handler on the root logger.
    """
    logger = logging.getLogger(name)
    if not any(
        isinstance(handler, CurrentStreamHandler) for handler in logger.handlers
    ):
        handler = CurrentStreamHandler(stream)
        handler.setFormatter(JsonFormatter(service))
        logger.addHandler(handler)
    logger.propagate = False
    logger.setLevel(level)

    return logger


def configure_logging(
    service: str, level: str = "INFO", stream: str = "stderr"
) -> None:
    """Send every log line, from this process's loggers, as JSON to stream."""
    handler = CurrentStreamHandler(stream)
    handler.setFormatter(JsonFormatter(service))

    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level)
