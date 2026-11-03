# Real-Time Programme Recommendation Pipeline

## 1. Goal and scope

Build a small-scale, event-driven recommendation pipeline that identifies trending programmes from synthetic viewing events. The system will ingest events continuously, aggregate viewing activity over rolling time windows, and publish ranked recommendations for downstream consumers.

The primary objective is to demonstrate reliable streaming-data engineering, including event-time processing, duplicate handling, fault recovery, automated testing, and measurable performance.

**In scope:** synthetic event generation, Kafka ingestion, Flink stream processing, sliding-window aggregation, DynamoDB persistence, a Lambda-based read API, automated tests, and performance measurements.

**Out of scope for the initial version:** personalised recommendations, production-scale infrastructure, real user data, and continue-watching recommendations. Continue-watching may be considered as a later extension.

## 2. Architecture

The project uses a hybrid local/cloud architecture.

- **Python:** synthetic event generation, test utilities, and performance measurement.
- **Kafka:** event ingestion and buffering.
- **Apache Flink:** stream processing, event-time windows, aggregation, and ranking.
- **AWS DynamoDB:** persistence of the latest recommendation results.
- **AWS Lambda:** API for retrieving the current trending programmes.
- **pytest:** unit, integration, and failure-handling tests.
- **Docker:** reproducible local development environment for Kafka and Flink.

The intended data flow is:

Synthetic event generator → Kafka → Flink → DynamoDB → Lambda read API → Client.

Kafka and Flink run locally during development. DynamoDB and Lambda run in AWS. Tests should support local execution wherever practical, with cloud integration tests separated from the default test suite.

## 3. Event schema

Each viewing event contains:

| Field | Type | Description |
|---|---|---|
| `event_id` | string | Globally unique synthetic event identifier |
| `user_id` | string | Synthetic viewer identifier |
| `programme_id` | string | Identifier from the invented programme catalogue |
| `event_type` | string | For example, `play`, `progress`, or `complete` |
| `event_time` | timestamp | UTC time when the viewing event occurred |
| `duration_seconds` | integer | Viewing duration associated with the event, where applicable |

All events use invented catalogue entries and generated user identifiers. The initial trending calculation counts eligible viewing events; it does not attempt to infer individual preferences.

## 4. Windowing and ranking

Flink calculates programme popularity using **one-hour sliding event-time windows, evaluated every five minutes**.

For each window, the pipeline scores each programme by its number of distinct viewers and selects the top 20 programmes. Results include the programme identifier, score, window start and end timestamps, and calculation timestamp.

Ranking is based on descending score, with `programme_id` as a deterministic tie-breaker. The initial implementation uses synthetic events and a clearly documented eligibility rule.

Events arriving late are handled according to a configurable event-time watermark and allowed-lateness policy. Events arriving beyond the accepted lateness threshold are recorded or measured rather than silently changing historical results.


### Event time and lateness

Windows use `event_time`, the moment the viewer's action happened, not the time a record reached Kafka or Flink.

- **Where event time comes from:** the producer copies `event_time` into the Kafka record timestamp, and the Flink Kafka source uses that timestamp for each record. After validation and deduplication the stream becomes a Table whose `event_time` column (`TIMESTAMP_LTZ(3)`) is the rowtime attribute, carrying the same watermarks.
- **Why the Kafka timestamp:** PyFlink ignores a Python `TimestampAssigner` passed to `from_source`, so the record timestamp is the only way to get watermarks generated per partition. Per-partition watermarks matter here: the generator compresses roughly 8 hours of event time into a 60-second run, so merging partitions before assigning watermarks would turn millisecond-level interleaving into minutes of apparent disorder.
- **Watermark:** for each partition, the newest `event_time` seen minus 30 seconds. The source's watermark is the minimum across active partitions.
- **Idleness:** a partition that delivers nothing for 60 seconds is marked idle and stops holding the watermark back. When it delivers again it rejoins.
- **What counts as late:** an event whose `event_time` is at or before the current watermark, meaning more than 30 seconds behind the newest event in its partition, or behind the others after its partition was idle.
- **What happens beyond the bound:** a late event still passes validation and deduplication. In a window aggregation it is counted if its window has not fired yet; if the window has already fired, the window operator drops it and counts it in its `numLateRecordsDropped` metric. Published results are never revised (no allowed lateness), which keeps historical results stable.
- **Malformed events:** when `event_time` is not an integer the producer stamps the record with `produced_at`, so a bad event cannot push a partition's watermark ahead. It is dead-lettered as before.
- **Known risk:** a well-formed but far-future `event_time` would advance its partition's watermark and make the events that follow late. Validation does not check plausibility yet.
- **Retention:** the event topics use `retention.ms=-1` locally, because Kafka's time-based retention reads record timestamps and synthetic event times can be weeks old.

### Trending results

- **Eligibility and score:** only `play_start` and `heartbeat` events count, since they mean someone is watching. A programme's score is `COUNT(DISTINCT user_id)` in the window, so one viewer sending many heartbeats still counts once.
- **Windows:** Flink SQL `HOP` windows, 60 minutes long, sliding every 5 minutes. Both are job arguments (`--window-size-minutes`, `--window-slide-minutes`, `--top-n`); the size must be a whole multiple of the slide.
- **Ranking:** Window Top-N with `ROW_NUMBER() OVER (PARTITION BY window_start, window_end ORDER BY score DESC, programme_id)`, keeping ranks 1 to 20. Each window emits its list once, when the watermark passes `window_end`.
- **Output:** one JSON message per ranked programme on `trending-results`:

  | Field | Meaning |
  |---|---|
  | `window_start`, `window_end` | Window bounds, ISO 8601 UTC (`2026-09-21T12:00:00Z`) |
  | `rank` | 1 = most distinct viewers |
  | `programme_id` | Ranked programme |
  | `score` | Distinct viewers in the window |
  | `max_produced_at` | Latest `produced_at` among the programme's events in the window (epoch ms) |
  | `emitted_at` | When Flink emitted the row (epoch ms) |

- **Quiet windows:** a window with no eligible events emits nothing, and the last windows of a run wait until the watermark passes their end.

## 5. Reliability and serving

Each event is deduplicated using `event_id` within a documented retention period. The pipeline must tolerate repeated delivery without inflating popularity counts. The design will document how deduplication state, checkpoints, and recovery interact.

Flink checkpoints and Kafka replay provide the intended recovery mechanism. DynamoDB writes should be idempotent for a given result-window identifier so that retries do not create duplicate logical results. The design does not assume end-to-end exactly-once delivery; the implementation must establish and test its actual guarantees.

Lambda exposes a read endpoint that returns the latest available trending results. The API returns a clear response when results are unavailable and does not perform the streaming aggregation itself.

### Serving table (DynamoDB)

The read path answers one question, "what is trending now?", so the table is designed around that query.

- **Table `trending`:** partition key `pk`, sort key `sk`, both strings, on-demand billing. Each window is one item: `pk = "TRENDING#1h"` (one partition for the 1-hour lists), `sk = window_end` as ISO 8601 UTC (`2026-09-21T18:10:00Z`), plus `items` (the ranked list of `rank`, `programme_id`, `score`), `generated_at` and `ttl`.
- **Latest list:** one `Query` on `pk = "TRENDING#1h"` with `ScanIndexForward=false` and `Limit=1`. ISO 8601 UTC strings sort in time order, so reading the partition backwards returns the newest window first.
- **Idempotent writes:** the window is the key, so writing the same window again replaces it instead of adding a copy.
- **Expiry:** `ttl` is `generated_at` plus 24 hours, in epoch seconds. It counts from when the list was written, not from `window_end`, because replayed or synthetic windows can already be days old. AWS deletes expired items lazily (typically within two days), so the latest-list query also filters on `#ttl > :now` and returns nothing rather than a stale list.
- **Reserved words:** `ttl` and `items` are DynamoDB reserved words, so expressions refer to them through placeholders such as `#ttl`.
- **One definition:** `infra/dynamodb/trending-table.json` is the only description of the table. `make db-init` creates it in DynamoDB Local, and the Terraform in RP-27 must read the same file so the two cannot drift.
- **Locally:** DynamoDB Local runs in Compose on port 8001 with `-sharedDb`, so every client sees one database whatever credentials or region it uses. Unlike AWS it deletes expired items within seconds.

### Checkpoints, restarts and exactly-once output

The job sets all of this itself (`flink_jobs/reliability.py`), so it travels with the job instead of depending on cluster settings.

- **Checkpoints:** every 10 seconds, exactly-once, with a 2-minute timeout, and kept when the job is cancelled. State (Kafka offsets, deduplication flags, window aggregates and open sink transactions) lives in the RocksDB state backend with incremental checkpoints, written to the `flink-checkpoints` volume at `/opt/flink/checkpoints`.
- **Exactly-once results:** the `trending-results` sink writes each checkpoint's rows in one Kafka transaction (transactional ID prefix `recommendation-pipeline-trending-results`) and commits it when the checkpoint completes. After a failure the job resumes from the last checkpoint, the open transaction is aborted, and its rows are written again, so a `read_committed` consumer sees each row exactly once. The cost is latency: rows become visible up to one checkpoint interval (10 s) after they are computed.
- **Transaction timeout:** 10 minutes, under the broker's `transaction.max.timeout.ms` of 15 minutes; the Kafka sink's default of 1 hour makes the job fail at start-up. A transaction is open for one interval plus at most the 2-minute checkpoint timeout, which leaves about 8 minutes for a restart to finish it before Kafka aborts it and those rows are lost.
- **Consumers:** downstream consumers read with `isolation.level=read_committed` (`common/kafka.py`). A `read_uncommitted` reader also sees rows from aborted transactions, so after a failure some windows can appear twice.
- **Dead letters:** the DLQ sink stays at-least-once. After a failure a few bad events can be dead-lettered twice, which is acceptable for a diagnostic topic and keeps it readable without transactions.
- **Restarts:** exponential backoff starting at 1 second, doubling up to 1 minute with 10% jitter, reset after 10 minutes without a failure.
- **Planned stops:** `make stop-job` stops the job with a savepoint under `/opt/flink/checkpoints/savepoints`, and `make submit-job SAVEPOINT=<path>` resumes from it. Stateful operators have fixed `uid`s so their state maps back on restore; the trending SQL must stay the same between stop and resume.
- **Known issue (PyFlink 1.20 with RocksDB):** cancelling the job, by `flink cancel` or as part of a failover, can crash the TaskManager JVM in RocksDB's native write path, because the Python worker's last state writes race with RocksDB shutting down. Stop-with-savepoint does not hit it. The TaskManager runs with `restart: unless-stopped`, so after a crash Docker restarts it and the restart strategy resumes the job from its last checkpoint. Use `make stop-job` rather than `flink cancel`. If the crashes matter more than RocksDB, `"state.backend.type": "hashmap"` avoids them; the state here is small.

### Results writer (Kafka to DynamoDB)

`writer/results_writer.py` (`make writer`) copies each window from `trending-results` into the `trending` table. Kafka already holds each committed row once; the writer has to keep it that way when it crashes and restarts.

- **Reading:** consumer group `results-writer` with `isolation.level=read_committed`, so rows from aborted Flink transactions are never seen, and auto-commit off. A new group starts from the earliest offset.
- **Restarting quickly:** the writer is a static group member (`group.instance.id=results-writer-1`). A writer that is killed never leaves its group, so without a fixed id the restarted one would wait for the dead one's 45-second session to time out before getting the partition. With it, the restarted writer takes the dead one's place at once. A second writer started with the same id fences off the first, so only one runs.
- **One item per window:** rows are buffered until their window is complete. Each window's rows come out of Flink in one transaction, and windows arrive in order on the topic's single partition, so a row from a different window, or 2 seconds without new rows, means the buffered window is complete. It is then written with one `put_item` keyed by `window_end`.
- **Idempotent:** the item depends only on the rows. `generated_at` is the window's latest `emitted_at`, when Flink emitted the list, not the writer's clock, so writing a replayed window again writes identical data. If a rank appears twice the later row wins, so an item never repeats a rank.
- **Commit after write:** offsets are committed synchronously, only after the window's item is written, and only up to the first row of the window still being read. A crash therefore replays the partly read window, plus the window just written if the crash came between its write and its commit; that one is rewritten unchanged. This is at-least-once delivery made effectively once by idempotent writes. Stopping with Ctrl-C does not commit the partly read window either.
- **Retries:** throttling (`ThrottlingException`, `ProvisionedThroughputExceededException`, `RequestLimitExceeded`) and 5xx errors are retried with exponential backoff and full jitter, a random wait of up to 0.1 s × 2^(try − 1), capped at 5 s, for at most 5 tries. boto3's own retries are switched off for the writer so that count is exact.
- **Writer DLQ:** a window that still fails after 5 tries, or fails with an error retrying cannot fix (such as a validation error), goes to `trending-writer-dlq` with the item, the error and the number of tries. Its offsets are committed only once the DLQ has the record. If DynamoDB cannot be reached at all, the writer stops without committing rather than dead-lettering every window, and a restart carries on from the last commit.
- **Checking it:** `make check-writer` rebuilds the items an uninterrupted writer would produce from `trending-results` and compares them with the table: no missing, extra or wrong windows, and no repeated ranks. Windows past their `ttl` are skipped, because DynamoDB deletes them.
- **Assumptions:** one partition and one writer. A window's rows are contiguous only within a partition, so with more partitions the buffer would have to track windows per partition.

### Deduplication

`DeduplicateEvents` keys the validated stream by `event_id` and keeps a `seen` flag per key in Flink `ValueState`. The first copy of an event is forwarded; later copies are dropped and counted in the `duplicate_events` metric.

**State TTL: 1 hour, processing time, counted from the first sighting** (`OnCreateAndWrite`, expired flags never returned).

- **Why not shorter:** duplicates come from client resends and producer retries (seconds) and from Kafka redelivery after a consumer restart or rebalance (seconds to minutes). One hour covers both with a wide margin.
- **Why not longer:** state grows with unique events per second multiplied by the TTL. At the 500 events/s test rate one hour is about 1.8 million keys (500 × 3,600), and every extra hour adds the same again. The heap state backend keeps all of it in TaskManager memory.
- **What it does not catch:** a copy that arrives more than an hour after the first one is counted again, for example a backlog replayed after an outage longer than the TTL. TTL uses processing time, so it measures time since Flink saw the event, not the event's `event_time`.
- **Interaction with checkpoints:** on failure, Flink restores the `seen` state and the Kafka offsets from the same checkpoint, so replayed events are judged against consistent state. Rows emitted after that checkpoint are emitted again; the trending sink writes them in a Kafka transaction that only commits with a checkpoint, so `read_committed` consumers still see each row once.
- **If the trade-off changes:** state lives in RocksDB on disk, so a longer TTL costs disk space rather than TaskManager memory, and event-time timers could expire state by `event_time` instead of wall-clock time.

### Testing against an oracle

`trending_oracle.py` is a plain-Python reference implementation of trending. It imports nothing from the Flink job and models the job's rules directly: drop repeat `event_id`s (first copy wins), keep a watermark of the newest `event_time` minus 30 seconds, fire a window once the watermark reaches `window_end` minus 1 ms, and count an event only in windows that have not fired when it arrives.

The slow tests run the job's own deduplication, rowtime table and trending SQL on an in-memory source in local PyFlink, with no Kafka, and require the output to equal the oracle's on every fixture: ties, late events, duplicates, empty windows, and a generated stream with duplicates and late events. One deliberate difference from production: the tests define the watermark in SQL and emit it after every record, because a bounded source finishes before Flink's periodic watermark fires and no event would ever be late.

`make test` skips these; `make test-flink` runs them, and CI runs them as their own job with Java installed.

## 6. Performance and acceptance criteria

The initial performance target is **p95 event-to-result latency of 30 seconds or less under a documented local test workload**, measured from event ingestion to the corresponding updated result becoming available. This is a provisional engineering target, not a production service-level agreement.

The implementation will measure:
- Sustained event throughput in events per second.
- p50, p95, and p99 processing latency.
- Duplicate-event handling and count correctness.
- Recovery behaviour following a simulated processing failure.
- Result freshness and successful DynamoDB writes.

Correctness takes priority over throughput. The project is successful when automated tests validate aggregation, window boundaries, ranking, duplicate handling, and recovery, and a repeatable benchmark documents the achieved performance.
