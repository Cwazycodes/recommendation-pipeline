"""Call the trending API's Lambda handler locally, against DynamoDB Local.

Builds the API Gateway HTTP API (payload 2.0) event for GET /trending, calls
the same handler Lambda runs, and prints the response.
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

from realtime_recommendation_pipeline.api.handler import default_table, handler


def http_api_event(query: Mapping[str, str]) -> dict[str, Any]:
    """The event API Gateway sends for GET /trending with these query parameters."""
    now = datetime.now(UTC)
    event: dict[str, Any] = {
        "version": "2.0",
        "routeKey": "GET /trending",
        "rawPath": "/trending",
        "rawQueryString": urlencode(query),
        "headers": {"accept": "application/json", "host": "localhost"},
        "requestContext": {
            "accountId": "local",
            "apiId": "local",
            "domainName": "localhost",
            "domainPrefix": "localhost",
            "http": {
                "method": "GET",
                "path": "/trending",
                "protocol": "HTTP/1.1",
                "sourceIp": "127.0.0.1",
                "userAgent": "api-local",
            },
            "requestId": f"local-{uuid.uuid4()}",
            "routeKey": "GET /trending",
            "stage": "$default",
            "time": now.strftime("%d/%b/%Y:%H:%M:%S +0000"),
            "timeEpoch": int(now.timestamp() * 1000),
        },
        "isBase64Encoded": False,
    }
    # API Gateway leaves queryStringParameters out when there is no query string.
    if query:
        event["queryStringParameters"] = dict(query)

    return event


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint-url", default="http://localhost:8001")
    parser.add_argument("--table", default="trending")
    parser.add_argument(
        "--limit", help="sent as given, so invalid values can be tried as well"
    )
    args = parser.parse_args(argv)

    # Configure the handler the way the Lambda function's environment will.
    os.environ["DYNAMODB_ENDPOINT_URL"] = args.endpoint_url
    os.environ["TABLE_NAME"] = args.table
    default_table.cache_clear()

    query = {} if args.limit is None else {"limit": args.limit}
    context = SimpleNamespace(aws_request_id=f"local-{uuid.uuid4()}")
    response = handler(http_api_event(query), context)

    print(f"HTTP {response['statusCode']}")
    print(json.dumps(json.loads(response["body"]), indent=2))
    sys.exit(0 if response["statusCode"] == 200 else 1)


if __name__ == "__main__":
    main()
