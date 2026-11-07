"""Create the table named by DYNAMODB_TABLE from infra/dynamodb/trending-table.json."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from pathlib import Path

from realtime_recommendation_pipeline.common.trending_table import (
    create_table_if_missing,
    dynamodb_resource,
    load_table_definition,
)
from realtime_recommendation_pipeline.settings import DynamoDBSettings, load


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--definition", type=Path, default=Path("infra/dynamodb/trending-table.json")
    )
    args = parser.parse_args(argv)

    dynamo = load(DynamoDBSettings)
    # The file gives the schema; the name comes from the settings.
    definition = {**load_table_definition(args.definition), "TableName": dynamo.table}
    client = dynamodb_resource(dynamo.endpoint_url).meta.client

    created = create_table_if_missing(client, definition)
    name = definition["TableName"]
    ttl = client.describe_time_to_live(TableName=name)["TimeToLiveDescription"]

    print(
        f"{'Created' if created else 'Found existing'} table {name} "
        f"at {dynamo.endpoint_url or 'AWS'}; TTL on '{ttl['AttributeName']}' is "
        f"{ttl['TimeToLiveStatus']}"
    )


if __name__ == "__main__":
    main()
