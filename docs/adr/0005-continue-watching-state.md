# 5. Continue watching: keyed state where the newest event wins

**Status:** accepted, 2026-11.

## Context

Continue watching is the other half of the brief: for each user, the programmes they started and haven't finished, with where they stopped. Trending counts viewers per programme over windows; this needs one value per user and programme, kept for as long as the user might come back, and changed by almost every event. Events can still arrive twice, late or out of order ([design.md](../design.md), section 7).

## Decision

- **A second Flink job,** `jobs/continue_watching.py`, reads the same events with its own consumer group, keys them by `user_id`, and keeps a `MapState` from each programme to the newest event seen for it.
- **The newest event wins:** an event replaces the kept one only if its `(event_time, event_id)` is greater. A repeat or an older event changes nothing, so the job needs no deduplication and no watermarks, and its final state doesn't depend on the order events arrive in.
- **More than 90% watched is finished.** The programme is removed, with a delete, and its newest event is kept, marked finished, so an older event arriving late can't bring it back. A newer event that isn't finished, someone watching it again, does.
- **Upserts and deletes,** one per change, go exactly once to `continue-watching-changes`, from one sink subtask to one partition, so each user's changes stay in order.
- **One item per user and programme,** in the same table as the trending lists: `pk = USER#<user_id>`, `sk = programme_id`. The writer applies each batch of changes with `BatchWriteItem`, keeping only the last change to each item, and commits after the write ([ADR 3](0003-idempotent-writes.md)).
- **Thirty days:** a programme's state expires 30 days after its last update, and its item carries a `ttl` 30 days after its last change.

## Alternatives considered

- **The last event to arrive wins:** simpler, but a late or repeated event would move a position backwards, and the result would depend on arrival order.
- **Windows, as for trending:** a position is a current value, not a count over a period, so windows would only add delay.
- **Keying by user and programme with `ValueState`:** the same rule. Keying by user, as the brief puts it, keeps a user's programmes in one place, which a per-user rule such as a limit on the list would need.
- **Publishing only on `pause` and `play_end`:** far fewer writes, but a closed laptop or a crash sends no pause, and the list would miss where people actually stopped.
- **Conditional writes in DynamoDB** (only if `event_time` is newer than the stored one): would make the writer order-independent, but a delete leaves nothing to compare with, so a late upsert could bring a finished programme back. The job already puts the changes in order.

## Consequences

- Nearly every event is a heartbeat that moves a position, so the job publishes about one change per event. The writer coalesces them: a batch holds many heartbeats for the same programmes, and only the last one per item is written.
- The keyed function runs in Python, so like deduplication ([ADR 2](0002-dedup-state-ttl.md)) it bounds throughput ([benchmarks.md](../benchmarks.md), "Continue watching").
- Finished programmes stay in state, as markers, for 30 days, so state grows with programmes watched, not only those in progress.
- A programme untouched for 30 days drops off the list. An event older than its finish that arrives more than 30 days later could bring a finished programme back; that is accepted.
- After a writer crash, the replayed batch can briefly put back an item that a later change in the same replay deletes again. The table is right once the replay is done.
