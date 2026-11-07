"""Compare the trending table with the windows on the trending topic.

The table should hold what one uninterrupted pass over the topic would write:
one item per window, with that window's latest ranked list and no repeated
ranks, however often the writer was killed and restarted. Windows past their
ttl are left out, because DynamoDB deletes them.
"""

from __future__ import annotations

import argparse
import sys
import time
import uuid
from collections.abc import Mapping, Sequence
from typing import Any

from boto3.dynamodb.conditions import Key

from realtime_recommendation_pipeline.common.kafka import downstream_consumer_config
from realtime_recommendation_pipeline.common.trending_table import (
    PARTITION_KEY,
    dynamodb_resource,
    plain,
)
from realtime_recommendation_pipeline.settings import (
    DynamoDBSettings,
    KafkaSettings,
    load,
)
from realtime_recommendation_pipeline.tools.check_trending import read_rows
from realtime_recommendation_pipeline.writer.windows import (
    WindowBuffer,
    window_to_item,
)


def expected_items(
    rows: Sequence[dict[str, Any]], partition_key: str = PARTITION_KEY
) -> dict[str, dict[str, Any]]:
    """Each window's item as a writer that never stopped would leave it."""
    buffer = WindowBuffer()
    items = {}

    for offset, row in enumerate(rows):
        # Offsets only matter for commits, so any partition label will do.
        complete = buffer.add(("rows", 0), offset, row)
        if complete is not None:
            items[complete.window_end] = window_to_item(complete, partition_key)

    last = buffer.take()
    if last is not None:
        items[last.window_end] = window_to_item(last, partition_key)

    return items


def table_items(
    table: Any, partition_key: str = PARTITION_KEY
) -> dict[str, dict[str, Any]]:
    """Every window item under partition_key, by window_end."""
    query: dict[str, Any] = {"KeyConditionExpression": Key("pk").eq(partition_key)}
    items = {}

    while True:
        response = table.query(**query)
        for item in response["Items"]:
            items[item["sk"]] = plain(item)

        if "LastEvaluatedKey" not in response:
            return items
        query["ExclusiveStartKey"] = response["LastEvaluatedKey"]


def compare(
    expected: Mapping[str, dict[str, Any]],
    actual: Mapping[str, dict[str, Any]],
    now_s: float,
) -> list[str]:
    """Missing, unexpected or wrong windows, ignoring expired ones."""
    live_expected = {end: item for end, item in expected.items() if item["ttl"] > now_s}
    live_actual = {end: item for end, item in actual.items() if item["ttl"] > now_s}

    problems = [
        f"missing: no item for {window_end}"
        for window_end in sorted(live_expected.keys() - live_actual.keys())
    ]
    problems += [
        f"unexpected: item for {window_end}, which is not on the topic"
        for window_end in sorted(live_actual.keys() - live_expected.keys())
    ]

    for window_end in sorted(live_expected.keys() & live_actual.keys()):
        ranks = [entry["rank"] for entry in live_actual[window_end]["items"]]
        if len(ranks) != len(set(ranks)):
            problems.append(f"duplicate: {window_end} repeats a rank")
        elif live_actual[window_end] != live_expected[window_end]:
            problems.append(f"wrong: {window_end} differs from the topic")

    return problems


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--idle-seconds", type=float, default=10.0)
    args = parser.parse_args(argv)

    kafka = load(KafkaSettings)
    dynamo = load(DynamoDBSettings)
    config = downstream_consumer_config(
        kafka.bootstrap_servers, group_id=f"check-writer-{uuid.uuid4()}"
    )
    rows = read_rows(config, kafka.trending_topic, args.idle_seconds)
    expected = expected_items(rows, dynamo.partition_key)
    table = dynamodb_resource(dynamo.endpoint_url).Table(dynamo.table)
    actual = table_items(table, dynamo.partition_key)

    now_s = time.time()
    live = sum(1 for item in expected.values() if item["ttl"] > now_s)
    print(
        f"{len(expected)} windows on {kafka.trending_topic} ({live} not expired), "
        f"{len(actual)} items in table {dynamo.table}"
    )

    problems = compare(expected, actual, now_s)
    for problem in problems:
        print(f"  {problem}")

    print("FAIL" if problems else "OK: one correct item per window, no duplicates")
    sys.exit(1 if problems else 0)


if __name__ == "__main__":
    main()
