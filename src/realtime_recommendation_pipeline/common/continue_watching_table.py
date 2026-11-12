"""Continue-watching items in the serving table: one per user and programme.

They share the trending table: pk is USER#<user_id> and sk the programme_id,
so one Query on a user's partition returns every programme they have in
progress, and the TRENDING# lists are never in the way.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from realtime_recommendation_pipeline.common.trending_table import millis_text, plain

USER_KEY_PREFIX = "USER#"
# As long as the job keeps a programme's progress (flink_jobs/continue_watching.py).
RETENTION = timedelta(days=30)

# The changes the job publishes (flink_jobs/continue_watching.py).
UPSERT = "upsert"
DELETE = "delete"


def user_key(user_id: str) -> str:
    return f"{USER_KEY_PREFIX}{user_id}"


def item_key(change: Mapping[str, Any]) -> dict[str, str]:
    return {"pk": user_key(change["user_id"]), "sk": change["programme_id"]}


def as_decimal(value: float | None) -> Decimal | None:
    """boto3 rejects floats; going through str keeps the value as written."""
    return None if value is None else Decimal(str(value))


def from_millis(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, UTC)


def progress_item(change: Mapping[str, Any]) -> dict[str, Any]:
    """The item an upsert writes, the same every time the change is replayed.

    ttl counts from emitted_at, when Flink published the change, not from
    event_time, which for replayed or synthetic events can be weeks old. A
    programme with no new change for 30 days leaves the table as it leaves the
    job's state.
    """
    duration_s = change["duration_s"]
    emitted_at = from_millis(change["emitted_at"])

    return {
        **item_key(change),
        "position_s": as_decimal(change["position_s"]),
        "duration_s": as_decimal(duration_s),
        "progress": (
            as_decimal(round(change["position_s"] / duration_s, 4))
            if duration_s
            else None
        ),
        "last_watched_at": millis_text(from_millis(change["event_time"])),
        "event_id": change["event_id"],
        "generated_at": millis_text(emitted_at),
        "ttl": int((emitted_at + RETENTION).timestamp()),
    }


def in_progress(table: Any, user_id: str, now: datetime) -> list[dict[str, Any]]:
    """The user's unexpired items, most recently watched first."""
    arguments: dict[str, Any] = {
        "KeyConditionExpression": "pk = :pk",
        # DynamoDB deletes expired items lazily, so they are filtered out.
        "FilterExpression": "#ttl > :now",
        "ExpressionAttributeNames": {"#ttl": "ttl"},
        "ExpressionAttributeValues": {
            ":pk": user_key(user_id),
            ":now": int(now.timestamp()),
        },
    }
    items: list[dict[str, Any]] = []
    while True:
        response = table.query(**arguments)
        items += [plain(item) for item in response["Items"]]
        if "LastEvaluatedKey" not in response:
            break
        arguments["ExclusiveStartKey"] = response["LastEvaluatedKey"]

    return sorted(items, key=lambda item: item["last_watched_at"], reverse=True)
