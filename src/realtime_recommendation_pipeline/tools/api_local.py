"""Call the read API's Lambda handler locally, against DynamoDB Local.

Builds the API Gateway HTTP API (payload 2.0) event for GET /trending, or for
GET /users/{id}/continue-watching with --user, calls the same handler Lambda
runs, and prints the response.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import uuid
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any
from urllib.parse import urlencode

from realtime_recommendation_pipeline.api.handler import configured, handler
from realtime_recommendation_pipeline.settings import DynamoDBSettings, load


def http_api_event(
    query: Mapping[str, str], user_id: str | None = None
) -> dict[str, Any]:
    """The event API Gateway sends for GET /trending with these query
    parameters, or for GET /users/{user_id}/continue-watching."""
    now = datetime.now(UTC)
    route, path = "GET /trending", "/trending"
    if user_id is not None:
        route = "GET /users/{id}/continue-watching"
        path = f"/users/{user_id}/continue-watching"
    event: dict[str, Any] = {
        "version": "2.0",
        "routeKey": route,
        "rawPath": path,
        "rawQueryString": urlencode(query),
        "headers": {"accept": "application/json", "host": "api-local"},
        "requestContext": {
            "accountId": "local",
            "apiId": "local",
            "domainName": "api-local",
            "domainPrefix": "api-local",
            "http": {
                "method": "GET",
                "path": path,
                "protocol": "HTTP/1.1",
                "sourceIp": "203.0.113.10",
                "userAgent": "api-local",
            },
            "requestId": f"local-{uuid.uuid4()}",
            "routeKey": route,
            "stage": "$default",
            "time": now.strftime("%d/%b/%Y:%H:%M:%S +0000"),
            "timeEpoch": int(now.timestamp() * 1000),
        },
        "isBase64Encoded": False,
    }
    # API Gateway leaves queryStringParameters out when there is no query string.
    if query:
        event["queryStringParameters"] = dict(query)
    if user_id is not None:
        event["pathParameters"] = {"id": user_id}

    return event


def call_handler(
    dynamo: DynamoDBSettings, limit: str | None = None, user_id: str | None = None
) -> dict[str, Any]:
    """The handler's response to GET /trending, or to a user's continue-watching
    list, configured as Lambda will be: through DYNAMODB_* environment variables.
    """
    os.environ["DYNAMODB_ENDPOINT_URL"] = dynamo.endpoint_url or ""
    os.environ["DYNAMODB_TABLE"] = dynamo.table
    os.environ["DYNAMODB_PARTITION_KEY"] = dynamo.partition_key
    configured.cache_clear()

    query = {} if limit is None else {"limit": limit}
    context = SimpleNamespace(aws_request_id=f"local-{uuid.uuid4()}")

    return handler(http_api_event(query, user_id), context)


def print_response(response: Mapping[str, Any]) -> None:
    print(f"HTTP {response['statusCode']}")
    print(json.dumps(json.loads(response["body"]), indent=2))


def main(argv: Sequence[str] | None = None) -> None:
    """The table, endpoint and partition key come from DYNAMODB_* settings."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--limit", help="sent as given, so invalid values can be tried as well"
    )
    parser.add_argument("--user", help="ask for this user's continue-watching list")
    args = parser.parse_args(argv)

    response = call_handler(load(DynamoDBSettings), args.limit, args.user)

    print_response(response)
    sys.exit(0 if response["statusCode"] == 200 else 1)


if __name__ == "__main__":
    main()
