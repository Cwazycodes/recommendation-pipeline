"""Read trending-results like a downstream consumer and check the ranked lists.

Reports gaps between consecutive lists and windows whose rows were written
more than once, which is what a restart would cause without exactly-once.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import uuid
from collections import Counter, defaultdict
from collections.abc import Sequence
from datetime import datetime, timedelta
from itertools import pairwise
from typing import Any

from confluent_kafka import Consumer, KafkaException

from realtime_recommendation_pipeline.common.kafka import downstream_consumer_config

TRENDING_TOPIC = "trending-results"
TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def check_lists(rows: Sequence[dict[str, Any]], slide_minutes: int) -> list[str]:
    """Problems with the ranked lists: gaps, repeated rows or broken ranks."""
    if not rows:
        return ["no trending rows found"]

    lists: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        lists[row["window_end"]].append(row)

    problems = []
    window_ends = sorted(lists)
    slide = timedelta(minutes=slide_minutes)

    for previous, current in pairwise(window_ends):
        step = datetime.strptime(current, TIME_FORMAT) - datetime.strptime(
            previous, TIME_FORMAT
        )
        if step != slide:
            problems.append(f"gap: no list between {previous} and {current}")

    for window_end in window_ends:
        ranks = Counter(row["rank"] for row in lists[window_end])
        repeated = sorted(rank for rank, count in ranks.items() if count > 1)

        if repeated:
            problems.append(f"duplicate: {window_end} has rank(s) {repeated} twice")
        if sorted(ranks) != list(range(1, len(ranks) + 1)):
            problems.append(f"broken ranks: {window_end} has {sorted(ranks)}")

    return problems


def read_rows(
    config: dict[str, str], topic: str, idle_s: float
) -> list[dict[str, Any]]:
    """Every row on the topic, stopping once nothing new arrives for idle_s."""
    consumer = Consumer(config)
    consumer.subscribe([topic])

    rows = []
    last_message_at = time.monotonic()

    try:
        while time.monotonic() - last_message_at < idle_s:
            message = consumer.poll(1.0)

            if message is None:
                continue
            if message.error():
                raise KafkaException(message.error())

            value = message.value()
            if value is not None:
                rows.append(json.loads(value))
            last_message_at = time.monotonic()
    finally:
        consumer.close()

    return rows


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument(
        "--isolation-level",
        choices=["read_committed", "read_uncommitted"],
        default="read_committed",
    )
    parser.add_argument("--slide-minutes", type=int, default=5)
    parser.add_argument("--idle-seconds", type=float, default=15.0)
    args = parser.parse_args(argv)

    config = downstream_consumer_config(
        args.bootstrap_servers, group_id=f"check-trending-{uuid.uuid4()}"
    )
    config["isolation.level"] = args.isolation_level

    rows = read_rows(config, TRENDING_TOPIC, args.idle_seconds)
    problems = check_lists(rows, args.slide_minutes)

    window_ends = sorted({row["window_end"] for row in rows})
    if window_ends:
        print(
            f"{len(rows)} rows in {len(window_ends)} ranked lists "
            f"({window_ends[0]} to {window_ends[-1]}), read {args.isolation_level}"
        )

    for problem in problems:
        print(f"  {problem}")

    print("FAIL" if problems else "OK: no gaps or duplicates")
    sys.exit(1 if problems else 0)


if __name__ == "__main__":
    main()
