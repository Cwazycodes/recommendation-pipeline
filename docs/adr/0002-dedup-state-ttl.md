# 2. Deduplication state expires after 1 hour of processing time

**Status:** accepted, 2026-11.

## Context

The same event can arrive twice: a client resends it, the producer retries it, or Kafka redelivers it after a consumer restart. The job drops repeats by `event_id`, which means remembering every `event_id` it has seen ([design.md](../design.md), "Deduplication"). Remembering them forever makes state grow without limit: at 500 events a second that is 1.8 million keys an hour, every hour.

## Decision

Key the validated stream by `event_id` and keep a `seen` flag per key in Flink keyed state, with a **state TTL of 1 hour of processing time, counted from the first sighting** (`OnCreateAndWrite`; expired flags are never returned). The first copy goes on; later copies are dropped and counted in the `duplicate_events` metric.

## Alternatives considered

- **No TTL:** exact for ever, but state grows for ever.
- **A shorter TTL, minutes:** enough for client resends and producer retries, which happen within seconds, but a replay after a long consumer restart could slip past it.
- **Expiry by event time, with timers:** expires keys relative to `event_time` rather than when Flink saw them, but costs a timer per key and more code than a TTL setting.
- **Deduplication in Flink SQL** (`ROW_NUMBER() OVER (PARTITION BY event_id ...) = 1` with a state TTL): the same rule, run in the JVM instead of a Python operator.

## Consequences

- A copy that arrives more than an hour after the first is counted again, for example from a backlog replayed after a long outage.
- On recovery the `seen` flags and the Kafka offsets come back from the same checkpoint, so replayed events are judged against consistent state ([reliability.md](../reliability.md), drill 1).
- The Python operator is the throughput bottleneck: about 150 to 190 events a second per subtask on the benchmark laptop ([benchmarks.md](../benchmarks.md)). Moving it into Flink SQL is the first change that would raise that.
- Today's score, `COUNT(DISTINCT user_id)`, cannot be inflated by a repeat of the same event, because the repeat has the same viewer. Deduplication keeps the guarantee independent of the score, so a score that counted events or watch time would still be right. If the score stays distinct viewers, dropping deduplication would trade that guarantee for throughput, and that would need a new record.
