"""Create the 'trending' table from infra/dynamodb/trending-table.json."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from pathlib import Path

from realtime_recommendation_pipeline.common.trending_table import (
    create_table_if_missing,
    dynamodb_resource,
    load_table_definition,
)


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint-url", default="http://localhost:8001")
    parser.add_argument(
        "--definition", type=Path, default=Path("infra/dynamodb/trending-table.json")
    )
    args = parser.parse_args(argv)

    definition = load_table_definition(args.definition)
    client = dynamodb_resource(args.endpoint_url).meta.client

    created = create_table_if_missing(client, definition)
    name = definition["TableName"]
    ttl = client.describe_time_to_live(TableName=name)["TimeToLiveDescription"]

    print(
        f"{'Created' if created else 'Found existing'} table {name} "
        f"at {args.endpoint_url}; TTL on '{ttl['AttributeName']}' is "
        f"{ttl['TimeToLiveStatus']}"
    )


if __name__ == "__main__":
    main()
