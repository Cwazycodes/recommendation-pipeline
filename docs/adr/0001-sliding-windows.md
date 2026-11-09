# 1. Sliding windows rather than tumbling windows

**Status:** accepted, 2026-11.

## Context

"Trending" means what people are watching now. The score is the number of distinct viewers of a programme in a window of event time ([design.md](../design.md), section 4). The window has to be long enough for the counts to mean something, since a few minutes of viewing gives small, noisy numbers, and the list has to change more often than the window is long, or "now" can be an hour stale.

## Decision

Flink SQL `HOP` windows, **60 minutes long, sliding every 5 minutes**, with Window Top-N keeping the 20 programmes with the most distinct viewers. Each window's list is emitted once, when the watermark passes its end, and stored as one item keyed by `window_end` ([0003](0003-idempotent-writes.md)). Size and slide are job arguments; the demo and the benchmarks use 2 minutes every 10 seconds so that a run shows results within a minute.

## Alternatives considered

- **Tumbling 1-hour windows:** the list would change once an hour, and every hour boundary would wipe the slate: a programme that took off at 10:50 would count for 10 minutes in one window, then start again from nothing at 11:00.
- **Tumbling 5-minute windows:** fresh, but each list would rest on 5 minutes of viewing, so the ranking would jump around with chance.
- **A running aggregate that updates with every event:** the freshest, but each update would retract and replace rows in the top 20, and every consumer, the results writer included, would have to handle those changes instead of whole lists.

## Consequences

- A new list every 5 minutes, each covering the last hour. It is published about 40 seconds after its window ends: the 30-second watermark delay, then up to one 10-second checkpoint before the rows are committed ([metrics.md](../metrics.md)).
- Every event counts towards 12 overlapping windows, so the aggregation does more work and keeps more state than one tumbling window would. At this scale that does not matter: in the benchmarks the window aggregation and ranking tasks stay under 100 ms busy a second while deduplication saturates ([benchmarks.md](../benchmarks.md)).
- Published lists never change: an event that arrives after its window fired is dropped and counted in `numLateRecordsDropped`, not added to an old list.
- The size must be a whole multiple of the slide, which the job checks.
