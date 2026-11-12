"""Lambda handler for the read API: GET /trending, the latest ranked list, and
GET /users/{id}/continue-watching, a user's in-progress programmes.

API Gateway (HTTP API, payload format 2.0) calls handler(event, context) for
both routes and names the route in routeKey. It only reads the table the
writers fill; it never aggregates anything.
Its settings come from DYNAMODB_* variables (settings.py). Besides boto3, which
the Lambda Python runtime provides, it needs pydantic-settings, so the
deployment package must include that, but nothing from Kafka or Flink.
"""

from __future__ import annotations

import json
import logging
import re
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from functools import cache
from typing import Any

from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from realtime_recommendation_pipeline.common.continue_watching_table import (
    in_progress,
)
from realtime_recommendation_pipeline.common.logs import service_logger
from realtime_recommendation_pipeline.common.trending_table import (
    PARTITION_KEY,
    dynamodb_resource,
    latest_window,
)
from realtime_recommendation_pipeline.settings import (
    DynamoDBSettings,
    LogSettings,
    load,
)

# One JSON line per request on stdout, which Lambda sends to CloudWatch Logs.
logger = service_logger(__name__, "trending-api", load(LogSettings).log_level)

TRENDING_ROUTE = "GET /trending"
CONTINUE_WATCHING_ROUTE = "GET /users/{id}/continue-watching"

DEFAULT_LIMIT = 10
# The Flink job ranks the top 20 programmes in each window.
MAX_LIMIT = 20
# A user can have any number of programmes in progress; one page is enough.
CONTINUE_WATCHING_MAX_LIMIT = 50
QUERY_PARAMETERS = frozenset({"limit"})
WHOLE_NUMBER = re.compile(r"[0-9]+")
# Generated ids look like user-00042.
USER_ID = re.compile(r"[A-Za-z0-9_-]{1,64}")

READ_FAILED = {
    TRENDING_ROUTE: "Trending results could not be read.",
    CONTINUE_WATCHING_ROUTE: "The continue-watching list could not be read.",
}

# Fail a request within seconds instead of waiting out boto3's defaults (a 60 s
# read timeout and several retries). Worst case: 2 tries of 1 s + 2 s.
API_CLIENT_CONFIG = Config(
    connect_timeout=1,
    read_timeout=2,
    retries={"mode": "standard", "total_max_attempts": 2},
)


class BadRequest(Exception):
    """A query parameter the API does not accept."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


@dataclass(frozen=True)
class Reply:
    """A JSON response, before it is shaped for API Gateway."""

    status: int
    body: dict[str, Any]
    headers: Mapping[str, str] = field(default_factory=dict)

    def to_api_gateway(self) -> dict[str, Any]:
        return {
            "statusCode": self.status,
            "headers": {"content-type": "application/json", **self.headers},
            "body": json.dumps(self.body),
            "isBase64Encoded": False,
        }


def error(status: int, code: str, message: str, **headers: str) -> Reply:
    return Reply(status, {"error": code, "message": message}, headers)


def parse_limit(query: Mapping[str, str] | None, maximum: int = MAX_LIMIT) -> int:
    """The limit query parameter, or DEFAULT_LIMIT. Raises BadRequest if invalid.

    API Gateway joins repeated parameters with commas (limit=5&limit=6 arrives
    as "5,6"), so a repeated limit is rejected as not a whole number.
    """
    query = query or {}

    unknown = sorted(set(query) - QUERY_PARAMETERS)
    if unknown:
        raise BadRequest(
            "unknown_parameter",
            f"Unknown query parameter: {', '.join(unknown)}. Only limit is supported.",
        )

    value = query.get("limit")
    if value is None:
        return DEFAULT_LIMIT

    if not WHOLE_NUMBER.fullmatch(value) or not 1 <= int(value) <= maximum:
        raise BadRequest(
            "invalid_limit", f"limit must be a whole number from 1 to {maximum}."
        )

    return int(value)


def route_of(event: Mapping[str, Any]) -> str:
    """The route API Gateway matched; GET /trending if the event names none."""
    return str(event.get("routeKey") or TRENDING_ROUTE)


def respond(
    event: Mapping[str, Any],
    table: Any,
    now: datetime,
    partition_key: str = PARTITION_KEY,
) -> Reply:
    """The reply to one request; DynamoDB errors are left to the caller."""
    method = event.get("requestContext", {}).get("http", {}).get("method")
    if method != "GET":
        return error(405, "method_not_allowed", "Only GET is supported.", allow="GET")

    if route_of(event) == CONTINUE_WATCHING_ROUTE:
        return continue_watching(event, table, now)
    return trending(event, table, now, partition_key)


def trending(
    event: Mapping[str, Any], table: Any, now: datetime, partition_key: str
) -> Reply:
    try:
        limit = parse_limit(event.get("queryStringParameters"))
    except BadRequest as bad:
        return error(400, bad.code, bad.message)

    latest = latest_window(table, now, partition_key)
    if latest is None:
        return error(404, "no_trending_data", "No trending results are available yet.")

    # Built field by field: DynamoDB does not keep a map's key order, and only
    # these fields are part of the API.
    items = [
        {
            "rank": entry["rank"],
            "programme_id": entry["programme_id"],
            "score": entry["score"],
        }
        for entry in latest["items"][:limit]
    ]

    return Reply(
        200,
        {
            "window_end": latest["sk"],
            "generated_at": latest["generated_at"],
            "limit": limit,
            "items": items,
        },
    )


def continue_watching(event: Mapping[str, Any], table: Any, now: datetime) -> Reply:
    user_id = (event.get("pathParameters") or {}).get("id", "")
    if not USER_ID.fullmatch(user_id):
        return error(
            400,
            "invalid_user_id",
            "The user id must be 1 to 64 letters, digits, hyphens or underscores.",
        )

    try:
        limit = parse_limit(
            event.get("queryStringParameters"), CONTINUE_WATCHING_MAX_LIMIT
        )
    except BadRequest as bad:
        return error(400, bad.code, bad.message)

    # Nothing in progress is an empty list, not a 404: every user has a list.
    items = [
        {
            "programme_id": entry["sk"],
            "position_s": entry["position_s"],
            "duration_s": entry["duration_s"],
            "progress": entry["progress"],
            "last_watched_at": entry["last_watched_at"],
        }
        for entry in in_progress(table, user_id, now)[:limit]
    ]

    return Reply(200, {"user_id": user_id, "limit": limit, "items": items})


def handle(
    event: Mapping[str, Any],
    table: Any,
    now: datetime | None = None,
    lambda_request_id: str | None = None,
    partition_key: str = PARTITION_KEY,
) -> dict[str, Any]:
    """Answer one API Gateway event and log it as a single JSON line."""
    started = time.perf_counter()
    route = route_of(event)
    outcome: dict[str, Any] = {}

    try:
        reply = respond(event, table, now or datetime.now(UTC), partition_key)
    except (BotoCoreError, ClientError) as exc:
        # DynamoDB unreachable, throttled or misconfigured. Anything else is a
        # bug and is left to raise, so Lambda counts it as a failed invocation.
        reply = error(500, "internal_error", READ_FAILED.get(route, "Read failed."))
        outcome = {"exception": type(exc).__name__, "detail": str(exc)}

    request = event.get("requestContext", {})
    items = reply.body.get("items")
    logger.log(
        logging.ERROR if reply.status >= 500 else logging.INFO,
        f"handled {route}",
        extra={
            "request_id": request.get("requestId"),
            "lambda_request_id": lambda_request_id,
            "method": request.get("http", {}).get("method"),
            "query": event.get("rawQueryString") or None,
            "status": reply.status,
            "error": reply.body.get("error"),
            "window_end": reply.body.get("window_end"),
            "user_id": reply.body.get("user_id"),
            "items": None if items is None else len(items),
            "duration_ms": round((time.perf_counter() - started) * 1000, 1),
            **outcome,
        },
    )

    return reply.to_api_gateway()


@cache
def configured() -> tuple[DynamoDBSettings, Any]:
    """The settings and the table, read once per Lambda container.

    DYNAMODB_ENDPOINT_URL points it at DynamoDB Local; empty, it uses AWS.
    """
    settings = load(DynamoDBSettings)
    dynamodb = dynamodb_resource(settings.endpoint_url, config=API_CLIENT_CONFIG)

    return settings, dynamodb.Table(settings.table)


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """Lambda entry point: realtime_recommendation_pipeline.api.handler.handler.

    DYNAMODB_PARTITION_KEY picks the list to serve: TRENDING#1h on the local stack.
    """
    settings, table = configured()
    return handle(
        event,
        table,
        lambda_request_id=getattr(context, "aws_request_id", None),
        partition_key=settings.partition_key,
    )
