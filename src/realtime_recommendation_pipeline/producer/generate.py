"""Generate deterministic synthetic programme viewing events."""

from __future__ import annotations

import argparse
import json
import random
from collections.abc import Iterator
from pathlib import Path
from uuid import UUID, uuid5

from realtime_recommendation_pipeline.common.events import (
    ViewingEvent,
    ViewingEventType,
)

NAMESPACE = UUID("d3a7e6d0-46ad-4d92-9b39-0e1e5f29c2e1")
BASE_TIME_MS = 1_790_000_000_000
HEARTBEAT_INTERVAL_S = 30

PROGRAMME_COUNT = 500
USER_COUNT = 1_000

PROGRAMME_NAMES = [
    "The Last Lighthouse",
    "Northern Skies",
    "Hidden Kingdom",
    "Midnight Signals",
    "The Quiet Planet",
    "River of Stars",
    "Beyond the Horizon",
    "The Forgotten Archive",
    "City After Dark",
    "Wild Frontiers",
]


def build_catalogue(rng: random.Random) -> list[dict[str, str]]:
    """Create an invented catalogue of programmes."""
    catalogue = []

    for index in range(PROGRAMME_COUNT):
        name = PROGRAMME_NAMES[index % len(PROGRAMME_NAMES)]
        programme_id = f"programme-{index + 1:04d}"

        catalogue.append(
            {
                "programme_id": programme_id,
                "title": f"{name} {index + 1:03d}",
            }
        )

    rng.shuffle(catalogue)
    return catalogue


def zipf_weights(count: int, exponent: float = 1.2) -> list[float]:
    """Return weights following a Zipf-like long-tail distribution."""
    return [1 / (rank**exponent) for rank in range(1, count + 1)]


def build_users() -> list[str]:
    """Create synthetic user identifiers."""
    return [f"user-{index + 1:05d}" for index in range(USER_COUNT)]


def make_event(
    *,
    seed: int,
    sequence: int,
    user_id: str,
    programme_id: str,
    event_type: ViewingEventType,
    position_s: float,
    duration_s: float,
    event_time: int,
    produced_at: int,
) -> dict[str, object]:
    """Create an event that conforms to the RP-07 contract."""
    event_id = uuid5(NAMESPACE, f"{seed}:{sequence}")

    event = ViewingEvent(
        event_id=event_id,
        schema_version=1,
        user_id=user_id,
        programme_id=programme_id,
        event_type=event_type,
        position_s=position_s,
        duration_s=duration_s,
        device="smart_tv",
        event_time=event_time,
        produced_at=produced_at,
    )

    return event.model_dump(mode="json")


def generate_valid_events(
    count: int,
    seed: int,
) -> list[dict[str, object]]:
    """Generate valid viewing events using complete synthetic sessions."""
    if count < 0:
        raise ValueError("count must be non-negative")

    rng = random.Random(seed)
    catalogue = build_catalogue(rng)
    users = build_users()

    weights = zipf_weights(len(catalogue))
    sequence = 0
    session_number = 0
    events: list[dict[str, object]] = []

    while len(events) < count:
        user_id = rng.choice(users)

        programme = rng.choices(
            catalogue,
            weights=weights,
            k=1,
        )[0]

        programme_id = programme["programme_id"]

        duration_s = float(rng.randint(600, 7_200))
        watched_s = rng.randint(30, int(duration_s))
        device = rng.choice(["smart_tv", "mobile", "tablet", "web"])

        # Space sessions apart to create realistic event times.
        session_start_ms = BASE_TIME_MS + session_number * 10_800_000

        def append_event(
            event_type: ViewingEventType,
            position_s: float,
            *,
            session_start_ms: int = session_start_ms,
            user_id: str = user_id,
            programme_id: str = programme_id,
            duration_s: float = duration_s,
            device: str = device,
        ) -> None:
            nonlocal sequence

            event_time = session_start_ms + int(position_s * 1_000)
            produced_at = event_time + rng.randint(0, 500)

            event = make_event(
                seed=seed,
                sequence=sequence,
                user_id=user_id,
                programme_id=programme_id,
                event_type=event_type,
                position_s=position_s,
                duration_s=duration_s,
                event_time=event_time,
                produced_at=produced_at,
            )

            event["device"] = device
            events.append(event)
            sequence += 1

        append_event(ViewingEventType.PLAY_START, 0)

        for position in range(
            HEARTBEAT_INTERVAL_S,
            watched_s,
            HEARTBEAT_INTERVAL_S,
        ):
            append_event(ViewingEventType.HEARTBEAT, float(position))

        terminal_type = rng.choice([ViewingEventType.PAUSE, ViewingEventType.PLAY_END])
        append_event(terminal_type, float(watched_s))

        # Increment once after the complete session.
        session_number += 1

    return events[:count]


def inject_faults(
    events: list[dict[str, object]],
    *,
    seed: int,
    dup_rate: float = 0.0,
    late_rate: float = 0.0,
    malformed_rate: float = 0.0,
) -> list[dict[str, object]]:
    """Inject duplicates, late events, and malformed events."""
    for rate in (dup_rate, late_rate, malformed_rate):
        if not 0.0 <= rate <= 1.0:
            raise ValueError("fault rates must be between 0 and 1")

    rng = random.Random(seed)
    output = [event.copy() for event in events]

    # Inject duplicates while preserving the original event ID.
    duplicated: list[dict[str, object]] = []

    for event in output:
        duplicated.append(event)

        if rng.random() < dup_rate:
            duplicated.append(event.copy())

    output = duplicated

    # Move selected events to the end of the stream.
    # Their event_time values remain unchanged.
    late_count = round(len(output) * late_rate)

    if late_count:
        late_indexes = set(rng.sample(range(len(output)), k=late_count))

        on_time = [
            event for index, event in enumerate(output) if index not in late_indexes
        ]

        late = [event for index, event in enumerate(output) if index in late_indexes]

        output = on_time + late

    # Malformed events are intentionally not Pydantic-validated.
    for event in output:
        if rng.random() < malformed_rate:
            event["event_time"] = "not-a-timestamp"

    return output


def generate_events(
    count: int,
    *,
    seed: int,
    dup_rate: float = 0.0,
    late_rate: float = 0.0,
    malformed_rate: float = 0.0,
) -> list[dict[str, object]]:
    """Generate valid events and apply configured fault injection."""
    events = generate_valid_events(count, seed)

    return inject_faults(
        events,
        seed=seed,
        dup_rate=dup_rate,
        late_rate=late_rate,
        malformed_rate=malformed_rate,
    )


def write_jsonl(
    events: Iterator[dict[str, object]],
    output: str,
) -> None:
    """Write events as JSON Lines to stdout or a file."""
    if output == "-":
        for event in events:
            print(json.dumps(event, separators=(",", ":")))
        return

    path = Path(output)
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8") as file:
        for event in events:
            file.write(json.dumps(event, separators=(",", ":")) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate synthetic programme viewing events."
    )

    parser.add_argument("--events", type=int, default=1_000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--dup-rate", type=float, default=0.0)
    parser.add_argument("--late-rate", type=float, default=0.0)
    parser.add_argument("--malformed-rate", type=float, default=0.0)
    parser.add_argument("--output", default="-")

    args = parser.parse_args()

    events = generate_events(
        args.events,
        seed=args.seed,
        dup_rate=args.dup_rate,
        late_rate=args.late_rate,
        malformed_rate=args.malformed_rate,
    )

    write_jsonl(iter(events), args.output)


if __name__ == "__main__":
    main()
