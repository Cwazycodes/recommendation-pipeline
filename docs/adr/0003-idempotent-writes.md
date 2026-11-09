# 3. Idempotent writes: one item per window, written before committing

**Status:** accepted, 2026-11.

## Context

Flink publishes each window's ranked rows to `trending-results` exactly once, in a Kafka transaction that commits with a checkpoint. The results writer copies them into DynamoDB, and it can crash at any moment: before a write, after it, or between the write and committing its Kafka offsets. No transaction spans Kafka and DynamoDB, so the writer cannot write and commit as one step.

## Decision

- **The window is the key.** Each window is one item: `pk` is the list (`TRENDING#1h`) and `sk` is `window_end`. Writing a window again replaces its item instead of adding one.
- **The item depends only on the rows.** `generated_at` is the latest `emitted_at` of the window's rows, not the writer's clock, so a replayed window produces the same item, apart from `written_at`, which records the write itself.
- **Write, then commit.** Offsets are committed synchronously after the item is written, and only up to the first row of the window still being read.

Together this is at-least-once delivery made effectively once ([design.md](../design.md), "Results writer").

## Alternatives considered

- **Commit, then write:** a crash after the commit loses the window for good.
- **Store the offsets in DynamoDB with the item,** in one transaction, and read them back on start: exactly-once without relying on idempotence, but the writer would manage its own offsets instead of its consumer group, and the table would carry Kafka bookkeeping.
- **Append a new item per write,** keyed by something unique: every replay would add a duplicate list, and readers would have to pick one.

## Consequences

- A crash replays the window being read, plus the window just written if the crash came before its commit. The replayed write leaves the same data, which the writer-kill drill and `test_kill_between_write_and_commit_rewrites_the_same_item` check ([reliability.md](../reliability.md), drill 3).
- The API's query is one `Query` with `Limit=1` on the newest `window_end`, because there is only ever one item per window.
- It relies on one partition and one writer: a window's rows are contiguous only within a partition. More partitions would need the writer to track windows per partition.
- Retryable DynamoDB errors are retried, and a window that still fails goes to a dead-letter topic before its offsets are committed, so nothing is skipped silently.
