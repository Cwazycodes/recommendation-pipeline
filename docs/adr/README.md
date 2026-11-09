# Architecture decision records

Short records of the decisions that shape the pipeline: what was decided, what else was considered, and what it costs. [design.md](../design.md) describes how the system works; these say why.

| ADR | Decision |
|---|---|
| [0001](0001-sliding-windows.md) | Rank programmes over 1-hour sliding windows, every 5 minutes |
| [0002](0002-dedup-state-ttl.md) | Remember each `event_id` for 1 hour of processing time |
| [0003](0003-idempotent-writes.md) | One DynamoDB item per window, written before the offsets are committed |
| [0004](0004-hybrid-local-aws.md) | Kafka and Flink on a laptop, storage and the API in AWS |

Each record has the same parts: context, decision, alternatives considered, and consequences. A decision that changes gets a new record that supersedes the old one, rather than an edit.
