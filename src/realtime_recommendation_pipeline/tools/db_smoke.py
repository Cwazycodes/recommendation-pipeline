"""Write two sample windows to the trending table and read the latest back.

Uses its own partition key, so the samples never show up as the real latest
list, and deletes them afterwards.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from datetime import UTC, datetime

from realtime_recommendation_pipeline.common.trending_table import (
    dynamodb_resource,
    latest_window,
    put_window,
    window_item,
)
from realtime_recommendation_pipeline.settings import DynamoDBSettings, load

SMOKE_PARTITION_KEY = "SMOKE#1h"

SAMPLE_ITEMS = [
    {"rank": 1, "programme_id": "programme-0165", "score": 27},
    {"rank": 2, "programme_id": "programme-0384", "score": 11},
    {"rank": 3, "programme_id": "programme-0157", "score": 6},
]


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args(argv)

    dynamo = load(DynamoDBSettings)
    table = dynamodb_resource(dynamo.endpoint_url).Table(dynamo.table)
    now = datetime.now(UTC)

    older = window_item(
        "2026-09-21T18:05:00Z", SAMPLE_ITEMS[:1], now, SMOKE_PARTITION_KEY
    )
    newer = window_item("2026-09-21T18:10:00Z", SAMPLE_ITEMS, now, SMOKE_PARTITION_KEY)

    try:
        put_window(table, older)
        put_window(table, newer)
        latest = latest_window(table, now, SMOKE_PARTITION_KEY)
    finally:
        for item in (older, newer):
            table.delete_item(Key={"pk": item["pk"], "sk": item["sk"]})

    print("Latest window read back:")
    print(json.dumps(latest, indent=2))

    if latest != newer:
        print("FAIL: the latest window is not the one written last")
        sys.exit(1)

    print("OK: wrote 2 sample windows and read the newest back unchanged")


if __name__ == "__main__":
    main()
